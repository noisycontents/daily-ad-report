// scripts/fetch-google.js
// Google Ads API 데이터 수집 및 Supabase 저장

import { google } from 'googleapis';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

// 환경변수 로드
dotenv.config();

// 환경변수 읽기 (Service Account 방식)
const GOOGLE_CLIENT_EMAIL = process.env.GOOGLE_CLIENT_EMAIL;
const GOOGLE_PRIVATE_KEY = process.env.GOOGLE_PRIVATE_KEY;
const GOOGLE_DEVELOPER_TOKEN = process.env.GOOGLE_DEVELOPER_TOKEN;
const GOOGLE_CUSTOMER_ID = process.env.GOOGLE_CUSTOMER_ID; // MCC 계정 ID (login-customer-id)
const GOOGLE_CLIENT_CUSTOMER_ID = process.env.GOOGLE_CLIENT_CUSTOMER_ID; // 실제 광고 계정 ID
const SUPA_URL = process.env.SUPA_URL;
const SUPA_KEY = process.env.SUPA_KEY;

// Supabase 클라이언트
const supa = createClient(SUPA_URL, SUPA_KEY);

// KST 기준 어제 날짜 계산
const getKSTYesterday = () => {
  const now = new Date();
  const kstOffset = 9 * 60 * 60 * 1000; // UTC+9
  const kstNow = new Date(now.getTime() + kstOffset);
  const kstYesterday = new Date(kstNow.getTime() - 24 * 60 * 60 * 1000);
  return kstYesterday.toISOString().slice(0, 10);
};

async function fetchGoogleData() {
  const yesterday = getKSTYesterday();
  
  console.log(`\n📅 구글 광고 데이터 수집 시작 (${yesterday})...`);

  // 환경변수 확인
  if (!GOOGLE_CLIENT_EMAIL || !GOOGLE_PRIVATE_KEY || !GOOGLE_DEVELOPER_TOKEN || !GOOGLE_CUSTOMER_ID) {
    console.error('❌ 구글 API 환경변수가 설정되지 않았습니다.');
    return;
  }

  console.log('🔧 환경변수 체크 완료');

  try {
    // 1) 인증 방식 선택 (OAuth2 vs Service Account)
    let accessToken;
    
    if (process.env.GOOGLE_REFRESH_TOKEN) {
      console.log('🔄 OAuth2 인증 방식 사용...');
      
      const oauth2Client = new google.auth.OAuth2(
        process.env.GOOGLE_CLIENT_ID,
        process.env.GOOGLE_CLIENT_SECRET,
        'http://localhost'
      );

      oauth2Client.setCredentials({
        refresh_token: process.env.GOOGLE_REFRESH_TOKEN
      });

      await oauth2Client.getAccessToken();
      accessToken = oauth2Client.credentials.access_token;
      
      console.log('🔐 OAuth2 인증 완료');
    } else {
      console.log('⚠️ Service Account 방식 사용 (권한 문제 발생 가능)');
      
      const auth = new google.auth.JWT({
        email: GOOGLE_CLIENT_EMAIL,
        key: GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n'),
        scopes: ['https://www.googleapis.com/auth/adwords']
      });

      await auth.authorize();
      accessToken = auth.credentials.access_token;
    }
    
    console.log('🔐 구글 인증 완료');
    console.log('🌐 구글 광고 API 호출 중...');

    // 2) Google Ads API REST 호출
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
      WHERE segments.date = '${yesterday}'
      AND campaign.status IN ('ENABLED', 'PAUSED')
      ORDER BY metrics.cost_micros DESC
    `;

    // MCC 계정 ID와 실제 광고 계정 ID 분리 (대시 제거)
    const mccCustomerId = GOOGLE_CUSTOMER_ID.replace(/-/g, ''); // MCC 계정 (login-customer-id)
    const clientCustomerId = GOOGLE_CLIENT_CUSTOMER_ID.replace(/-/g, ''); // 실제 광고 계정 (API 엔드포인트)
    
    // Google Ads API v20 REST 엔드포인트 (실제 광고 계정 ID 사용)
    const apiUrl = `https://googleads.googleapis.com/v20/customers/${clientCustomerId}/googleAds:search`;
    
    const requestBody = {
      query: query
    };

    const response = await fetch(apiUrl, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'developer-token': GOOGLE_DEVELOPER_TOKEN,
        'login-customer-id': mccCustomerId, // MCC 계정 ID 사용
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
    const regularCampaigns = results.filter(row => 
      row.campaign.advertisingChannelType !== 'PERFORMANCE_MAX'
    );
    const pmaxCampaigns = results.filter(row => 
      row.campaign.advertisingChannelType === 'PERFORMANCE_MAX'
    );

    console.log(`📊 일반 캠페인: ${regularCampaigns.length}건, PMAX 캠페인: ${pmaxCampaigns.length}건`);

    // 데이터 집계 함수
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
        
        // 검색 노출 점유율이 있는 캠페인만 평균 계산
        if (metrics.searchImpressionShare) {
          totalSearchImprShare += Number(metrics.searchImpressionShare || 0);
          searchImprShareCount++;
        }
      });

      // 집계된 지표 계산
      const ctr = totalImpressions ? totalClicks / totalImpressions : 0;
      const cpc = totalClicks ? totalSpend / totalClicks : 0;
      const cvr = totalClicks ? totalConversion / totalClicks : 0;
      const cpm = totalImpressions ? (totalSpend / totalImpressions) * 1000 : 0;
      const cpa = totalConversion ? totalSpend / totalConversion : 0;
      const roas = totalSpend ? totalConversionValue / totalSpend : 0;
      const aov = totalConversion ? totalConversionValue / totalConversion : 0;
      const searchImprShare = searchImprShareCount ? (totalSearchImprShare / searchImprShareCount) * 100 : 0;

      return {
        date: yesterday,
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
        quality_score: 0, // 집계 레벨에서는 0
        top_impr_rate: 0 // 별도 쿼리 필요
      };
    }

    // 4) 집계된 데이터 생성
    const rows = [];
    
    // GoogleSA (일반 캠페인 집계)
    const googleSAData = aggregateData(regularCampaigns, 'GoogleSA');
    if (googleSAData) {
      rows.push(googleSAData);
    }

    // PMAX (Performance Max 캠페인 집계)
    const pmaxData = aggregateData(pmaxCampaigns, 'PMAX');
    if (pmaxData) {
      rows.push(pmaxData);
    }

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
      console.log(`✅ ${yesterday} 구글 데이터 ${rows.length}건 upsert 완료`);
    } else {
      console.log('⚠️ 저장할 구글 데이터가 없습니다.');
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