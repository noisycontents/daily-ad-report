// scripts/fetch-meta.js

import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

// 환경변수 로드
dotenv.config();

// 환경변수 읽기
const META_TOKEN = process.env.META_TOKEN;
const AD_ACCOUNT = process.env.AD_ACCOUNT;
const SUPA_URL   = process.env.SUPA_URL;
const SUPA_KEY   = process.env.SUPA_KEY;

// 환경변수 확인
console.log('🔧 환경변수 체크:');
console.log('META_TOKEN:', META_TOKEN ? '✅ 설정됨' : '❌ 없음');
console.log('AD_ACCOUNT:', AD_ACCOUNT ? '✅ 설정됨' : '❌ 없음');
console.log('SUPA_URL:', SUPA_URL ? '✅ 설정됨' : '❌ 없음');
console.log('SUPA_KEY:', SUPA_KEY ? '✅ 설정됨' : '❌ 없음');

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

async function fetchAndUpsert() {
  const yesterday = getKSTYesterday();
  console.log(`\n📅 ${yesterday} 데이터 수집 시작 (KST 기준 어제)...`);

  // 1) Meta API 호출: action_values 필드 추가
  const url = `https://graph.facebook.com/v16.0/${AD_ACCOUNT}/insights` +
              `?time_range={'since':'${yesterday}','until':'${yesterday}'}` +
              `&fields=date_start,spend,impressions,clicks,actions,action_values,cost_per_action_type` +
              `&access_token=${META_TOKEN}`;

  console.log('🌐 Meta API 호출 중...');
  const res = await fetch(url);
  if (!res.ok) {
    const errorText = await res.text();
    console.error('❌ Meta API 에러:', res.status, res.statusText);
    console.error('응답 내용:', errorText);
    throw new Error(`Meta API error: ${res.status} ${res.statusText}`);
  }
  
  const responseData = await res.json();
  console.log('📊 Meta API 응답:', responseData);
  const { data } = responseData;

  // 2) 각 행별로 누락된 지표 계산
  const rows = data.map(r => {
    const date         = r.date_start;
    const spend        = Number(r.spend);
    const impressions  = Number(r.impressions);
    const clicks       = Number(r.clicks);

    // 구매 관련: actions(건수)와 action_values(금액) 분리
    const purchaseCountAction = (r.actions || []).find(a => a.action_type === 'purchase');
    const purchaseValueAction = (r.action_values || []).find(a => a.action_type === 'purchase');
    
    const purchaseCount  = purchaseCountAction ? Number(purchaseCountAction.value) : 0;
    const purchaseValue  = purchaseValueAction ? Number(purchaseValueAction.value) : 0;

    // CPA fallback
    const cpaEntry = (r.cost_per_action_type || []).find(a => a.action_type === 'purchase');
    const CPA = cpaEntry
      ? Number(cpaEntry.value)
      : (purchaseCount ? spend / purchaseCount : 0);

    // 나머지 지표 계산
    const CTR  = impressions ? clicks / impressions : 0;
    const CPC  = clicks ? spend / clicks : 0;
    const CVR  = clicks ? purchaseCount / clicks : 0;
    const CPM  = impressions ? (spend / impressions) * 1000 : 0;
    const ROAS = spend ? (purchaseValue / spend) : 0;
    const AOV  = purchaseCount ? (purchaseValue / purchaseCount) : 0;

    return {
      date,            // 날짜
      campaign: 'daily-auto-fetch', // 캠페인명 (기본값)
      spend,           // 광고비
      impressions,     // 노출
      clicks,          // 클릭수
      ctr: CTR,        // 클릭률 (소문자)
      cpc: CPC,        // 클릭당비용 (소문자)
      purchase:        purchaseCount,    // 구매 건수
      purchase_value:  purchaseValue,    // 구매금액 합계
      roas: ROAS,      // 광고수익률 (소문자)
      cvr: CVR,        // 전환율 (소문자)
      cpm: CPM,        // 천회노출단가 (소문자)
      cpa: CPA,        // 액션당비용 (소문자)
      aov: AOV,        // 평균주문금액 (소문자)
    };
  });

  console.log(`📝 처리된 데이터 (${rows.length}건):`, rows);

  // 3) Supabase upsert (date, campaign 기준 중복 방지)
  if (rows.length > 0) {
    const now = new Date().toISOString();
    rows.forEach(row => {
      row.updated_at = now;
    });
  }
  console.log('💾 Supabase에 데이터 저장 중...');
  const { data: upsertData, error } = await supa
    .from('meta_insights')
    .upsert(rows, { onConflict: ['date', 'campaign'] });

  if (error) {
    console.error('❌ Supabase 에러:', error);
    throw error;
  }
  
  console.log('💾 Supabase 응답:', upsertData);
  console.log(`✅ ${yesterday} 데이터 ${rows.length}건 upsert 완료`);
}

// 스크립트 직접 실행 시 (ES modules 방식)
if (import.meta.url === `file://${process.argv[1]}`) {
  fetchAndUpsert().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

// 함수 export (통합 스크립트에서 사용)
export { fetchAndUpsert as fetchMetaData }; 