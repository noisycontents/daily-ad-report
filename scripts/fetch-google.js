// scripts/fetch-google.js
// Google Ads API 데이터 수집 및 Supabase 저장

import { google } from 'googleapis';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

// 환경변수 로드
dotenv.config();

// OAuth2 설치형/웹앱 플로우 환경변수
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID;
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET;
const GOOGLE_REFRESH_TOKEN = process.env.GOOGLE_REFRESH_TOKEN;

// 기타 Google Ads 설정
const GOOGLE_DEVELOPER_TOKEN = process.env.GOOGLE_DEVELOPER_TOKEN;
const GOOGLE_CUSTOMER_ID = process.env.GOOGLE_CUSTOMER_ID; // MCC 계정 ID (login-customer-id)
const GOOGLE_CLIENT_CUSTOMER_ID = process.env.GOOGLE_CLIENT_CUSTOMER_ID; // 실제 광고 계정 ID
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_KEY;

// Supabase 클라이언트
const supa = createClient(SUPABASE_URL, SUPABASE_KEY);

// KST 기준 어제 날짜 계산
const getKSTYesterday = () => {
  const now = new Date();
  const kstOffset = 9 * 60 * 60 * 1000; // UTC+9
  const kstNow = new Date(now.getTime() + kstOffset);
  const kstYesterday = new Date(kstNow.getTime() - 24 * 60 * 60 * 1000);
  return kstYesterday.toISOString().slice(0, 10);
};

// 0) 테스트용 날짜 설정 (비워두면 어제 날짜로 작동)
const testDates = []; // 테스트할 날짜들 (비워두면 어제 날짜 사용)

async function fetchGoogleData() {
  const datesToRun = (Array.isArray(testDates) && testDates.length > 0)
    ? testDates
    : [getKSTYesterday()];

  console.log(`\n📅 구글 광고 데이터 수집 시작 (총 ${datesToRun.length}개 날짜)`);

  // 환경변수 확인 (OAuth2 전용)
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN) {
    console.error('❌ OAuth2 환경변수(GOOGLE_CLIENT_ID/GOOGLE_CLIENT_SECRET/GOOGLE_REFRESH_TOKEN)가 설정되지 않았습니다.');
    return;
  }
  if (!GOOGLE_DEVELOPER_TOKEN) {
    console.error('❌ GOOGLE_DEVELOPER_TOKEN 이(가) 설정되지 않았습니다.');
    return;
  }
  if (!GOOGLE_CUSTOMER_ID) {
    console.error('❌ GOOGLE_CUSTOMER_ID (MCC ID)가 설정되지 않았습니다.');
    return;
  }
  if (!GOOGLE_CLIENT_CUSTOMER_ID) {
    console.error('❌ GOOGLE_CLIENT_CUSTOMER_ID (클라이언트 광고계정 ID)가 설정되지 않았습니다.');
    return;
  }

  console.log('🔧 환경변수 체크 완료');

  try {
    // 1) 인증 방식: OAuth2 설치형/웹앱 플로우 (Service Account 미사용)
    console.log('🔄 OAuth2 인증 방식 사용...');
    const oauth2Client = new google.auth.OAuth2(
      GOOGLE_CLIENT_ID,
      GOOGLE_CLIENT_SECRET,
      'http://localhost'
    );

    oauth2Client.setCredentials({
      refresh_token: GOOGLE_REFRESH_TOKEN
    });

    await oauth2Client.getAccessToken();
    const accessToken = oauth2Client.credentials.access_token;
    if (!accessToken) {
      throw new Error('액세스 토큰을 발급받지 못했습니다. 리프레시 토큰과 클라이언트 설정을 확인하세요.');
    }

    console.log('🔐 OAuth2 인증 완료');
    console.log('🌐 구글 광고 API 호출 중...');

    // 공통 상수 준비
    const mccCustomerId = GOOGLE_CUSTOMER_ID.replace(/-/g, ''); // MCC 계정 (login-customer-id)
    const clientCustomerId = GOOGLE_CLIENT_CUSTOMER_ID.replace(/-/g, ''); // 실제 광고 계정 (API 엔드포인트)
    const apiUrl = `https://googleads.googleapis.com/v20/customers/${clientCustomerId}/googleAds:search`;

    for (const targetDate of datesToRun) {
      console.log(`\n📅 처리 날짜: ${targetDate}`);

      // 2) Google Ads API REST 호출 (날짜별)
      const query = `
        SELECT 
          campaign.id,
          campaign.name,
          campaign.status,
          campaign.advertising_channel_type,
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros,
          metrics.conversions,
          metrics.conversions_value,
          metrics.search_impression_share,
          metrics.ctr,
          metrics.average_cpc,
          metrics.cost_per_conversion,
          metrics.conversions_from_interactions_rate
        FROM campaign 
        WHERE segments.date = '${targetDate}'
        AND campaign.status IN ('ENABLED', 'PAUSED')
        ORDER BY metrics.cost_micros DESC
      `;

      const requestBody = { query };

      const response = await fetch(apiUrl, {
        method: 'POST',
        headers: {
          'Authorization': `Bearer ${accessToken}`,
          'developer-token': GOOGLE_DEVELOPER_TOKEN,
          'login-customer-id': mccCustomerId,
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error('❌ Google Ads API 응답 에러:', response.status, errorText);
        throw new Error(`Google Ads API Error: ${response.status} - ${errorText}`);
      }

      // JSON 응답 처리
      const responseData = await response.json();
      const results = responseData.results || [];

      console.log('📊 구글 광고 API 응답 받음:', results.length, '건');

      // 3) 캠페인 타입별로 데이터 분류 및 집계
      // PMAX + 디맨드젠(DEMAND_GEN, 구 DISCOVERY)을 하나의 PMAX 묶음으로 합산
      const isPmaxOrDemandGen = (row) => {
        const type = row.campaign.advertisingChannelType;
        return type === 'PERFORMANCE_MAX' || type === 'DEMAND_GEN' || type === 'DISCOVERY';
      };

      const pmaxCampaigns = results.filter(isPmaxOrDemandGen);
      const regularCampaigns = results.filter(row => !isPmaxOrDemandGen(row));

      console.log(`📊 일반 캠페인: ${regularCampaigns.length}건, PMAX(+디맨드젠): ${pmaxCampaigns.length}건`);

      // 데이터 집계 함수 (날짜 클로저 사용)
      function aggregateData(campaigns, campaignName) {
        if (campaigns.length === 0) return null;

        let totalSpend = 0;
        let totalImpressions = 0;
        let totalClicks = 0;
        let totalConversion = 0;
        let totalConversionValue = 0;
        let totalSearchImprShare = 0;
        let searchImprShareCount = 0;

        campaigns.forEach(row => {
          const metrics = row.metrics;
          totalSpend += Number(metrics.costMicros || 0) / 1000000;
          totalImpressions += Number(metrics.impressions || 0);
          totalClicks += Number(metrics.clicks || 0);
          totalConversion += Number(metrics.conversions || 0);
          totalConversionValue += Number(metrics.conversionsValue || 0);
          
          if (metrics.searchImpressionShare) {
            totalSearchImprShare += Number(metrics.searchImpressionShare || 0);
            searchImprShareCount++;
          }
        });

        const ctr = totalImpressions ? totalClicks / totalImpressions : 0;
        const cpc = totalClicks ? totalSpend / totalClicks : 0;
        const cvr = totalClicks ? totalConversion / totalClicks : 0;
        const cpm = totalImpressions ? (totalSpend / totalImpressions) * 1000 : 0;
        const cpa = totalConversion ? totalSpend / totalConversion : 0;
        const roas = totalSpend ? totalConversionValue / totalSpend : 0;
        const aov = totalConversion ? totalConversionValue / totalConversion : 0;
        const searchImprShare = searchImprShareCount ? (totalSearchImprShare / searchImprShareCount) * 100 : 0;

        return {
          date: targetDate,
          campaign: campaignName,
          spend: totalSpend,
          impressions: totalImpressions,
          clicks: totalClicks,
          ctr,
          cpc,
          conversion: totalConversion,
          conversion_value: totalConversionValue,
          roas,
          cvr,
          cpm,
          cpa,
          aov,
          search_impr_share: searchImprShare,
          quality_score: 0,
          top_impr_rate: 0
        };
      }

      // 4) 집계된 데이터 생성
      const rows = [];
      const googleSAData = aggregateData(regularCampaigns, 'GoogleSA');
      if (googleSAData) rows.push(googleSAData);
      const pmaxData = aggregateData(pmaxCampaigns, 'PMAX');
      if (pmaxData) rows.push(pmaxData);

      console.log(`📝 처리된 구글 데이터 (${rows.length}건):`, rows);

      // 5) Supabase upsert
      if (rows.length > 0) {
        console.log('💾 Supabase에 구글 데이터 저장 중...');
        const { data: upsertData, error } = await supa
          .from('google_insights')
          .upsert(rows, { onConflict: ['date', 'campaign'] });

        if (error) {
          console.error('❌ Supabase 에러:', error);
          throw error;
        }

        console.log('💾 Supabase 응답:', upsertData);
        console.log(`✅ ${targetDate} 구글 데이터 ${rows.length}건 upsert 완료`);
      } else {
        console.log('⚠️ 저장할 구글 데이터가 없습니다.');
      }
    }

  } catch (error) {
    console.error('💥 구글 API 에러:', error);
    
    // 상세 에러 정보 출력
    if (error.response) {
      console.error('📋 응답 상태:', error.response.status);
      console.error('📋 응답 데이터:', error.response.data);
    }
    
    throw error;
  }
}

// 스크립트 직접 실행 시
if (import.meta.url === `file://${process.argv[1]}`) {
  fetchGoogleData().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

export { fetchGoogleData }; 