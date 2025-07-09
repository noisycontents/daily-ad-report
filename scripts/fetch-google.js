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
const GOOGLE_CLIENT_CUSTOMER_ID = '738-465-6133'; // 실제 광고 계정 ID
const SUPA_URL = process.env.SUPA_URL;
const SUPA_KEY = process.env.SUPA_KEY;

// Supabase 클라이언트
const supa = createClient(SUPA_URL, SUPA_KEY);

async function fetchGoogleData() {
  const today = new Date().toISOString().slice(0, 10);
  const yesterday = new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString().slice(0, 10);
  
  console.log(`\n📅 구글 광고 데이터 수집 시작 (${yesterday})...`);

  // 환경변수 확인
  console.log('🔧 구글 API 환경변수 체크:');
  console.log('GOOGLE_CLIENT_EMAIL:', GOOGLE_CLIENT_EMAIL ? '✅ 설정됨' : '❌ 없음');
  console.log('GOOGLE_PRIVATE_KEY:', GOOGLE_PRIVATE_KEY ? '✅ 설정됨' : '❌ 없음');
  console.log('GOOGLE_DEVELOPER_TOKEN:', GOOGLE_DEVELOPER_TOKEN ? '✅ 설정됨' : '❌ 없음');
  console.log('GOOGLE_CUSTOMER_ID (MCC):', GOOGLE_CUSTOMER_ID ? '✅ 설정됨' : '❌ 없음');
  console.log('GOOGLE_CLIENT_CUSTOMER_ID (광고계정):', GOOGLE_CLIENT_CUSTOMER_ID ? '✅ 설정됨' : '❌ 없음');

  if (!GOOGLE_CLIENT_EMAIL || !GOOGLE_PRIVATE_KEY || !GOOGLE_DEVELOPER_TOKEN || !GOOGLE_CUSTOMER_ID) {
    console.error('❌ 구글 API 환경변수가 설정되지 않았습니다.');
    return;
  }

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
      AND campaign.status = 'ENABLED'
      ORDER BY metrics.cost_micros DESC
    `;

    // MCC 계정 ID와 실제 광고 계정 ID 분리 (대시 제거)
    const mccCustomerId = GOOGLE_CUSTOMER_ID.replace(/-/g, ''); // MCC 계정 (login-customer-id)
    const clientCustomerId = GOOGLE_CLIENT_CUSTOMER_ID.replace(/-/g, ''); // 실제 광고 계정 (API 엔드포인트)
    
    console.log(`🏢 MCC 계정 ID: ${mccCustomerId}`);
    console.log(`📊 광고 계정 ID: ${clientCustomerId}`);
    
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

    // 3) 데이터 변환 및 지표 계산
    const rows = results.map(row => {
      const campaign = row.campaign;
      const metrics = row.metrics;

      // 기본 데이터 (Google은 마이크로 단위로 제공)
      const spend = Number(metrics.costMicros || 0) / 1000000; // 마이크로 → 원화
      const impressions = Number(metrics.impressions || 0);
      const clicks = Number(metrics.clicks || 0);
      const conversion = Number(metrics.conversions || 0);
      const conversionValue = Number(metrics.conversionsValue || 0);

      // 기본 지표 계산 (Google API에서 일부 제공되지만 일관성을 위해 직접 계산)
      const ctr = impressions ? clicks / impressions : 0;
      const cpc = clicks ? spend / clicks : 0;
      const cvr = clicks ? conversion / clicks : 0;
      const cpm = impressions ? (spend / impressions) * 1000 : 0;
      const cpa = conversion ? spend / conversion : 0;
      const roas = spend ? conversionValue / spend : 0;
      const aov = conversion ? conversionValue / conversion : 0;

      // 구글 특화 지표
      const searchImprShare = Number(metrics.searchImpressionShare || 0) * 100; // 퍼센트로 변환
      const qualityScore = 0; // 키워드 레벨에서만 제공되므로 캠페인 레벨에서는 0
      const topImprRate = 0; // 별도 쿼리 필요

      return {
        date: yesterday,
        campaign: campaign.name,
        campaign_id: campaign.id.toString(),
        spend,
        impressions,
        clicks,
        ctr,
        cpc,
        conversion,
        conversion_value: conversionValue,
        roas,
        cvr,
        cpm,
        cpa,
        aov,
        search_impr_share: searchImprShare,
        quality_score: qualityScore,
        top_impr_rate: topImprRate
      };
    });

    console.log(`📝 처리된 구글 데이터 (${rows.length}건):`, rows);

    // 4) Supabase upsert
    if (rows.length > 0) {
      console.log('💾 Supabase에 구글 데이터 저장 중...');
      const { data: upsertData, error } = await supa
        .from('google_insights')
        .upsert(rows, { onConflict: ['date', 'campaign_id'] });

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