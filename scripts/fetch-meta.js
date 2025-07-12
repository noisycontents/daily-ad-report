// scripts/fetch-meta.js

import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

// 환경변수 로드
dotenv.config();

// 환경변수 읽기
const META_TOKEN = process.env.META_TOKEN;
const META_AD_ACCOUNT = process.env.META_AD_ACCOUNT;
const SUPA_URL   = process.env.SUPA_URL;
const SUPA_KEY   = process.env.SUPA_KEY;

// 환경변수 확인
console.log('🔧 환경변수 체크:');
console.log('META_TOKEN:', META_TOKEN ? '✅ 설정됨' : '❌ 없음');
console.log('META_AD_ACCOUNT:', META_AD_ACCOUNT ? '✅ 설정됨' : '❌ 없음');
console.log('SUPA_URL:', SUPA_URL ? '✅ 설정됨' : '❌ 없음');
console.log('SUPA_KEY:', SUPA_KEY ? '✅ 설정됨' : '❌ 없음');

// 디버깅: 환경변수 값 일부 표시 (보안을 위해 일부만)
console.log('🔍 환경변수 값 확인:');
console.log('META_TOKEN 길이:', META_TOKEN ? META_TOKEN.length : 0);
console.log('META_AD_ACCOUNT 값:', META_AD_ACCOUNT || '(없음)');
console.log('SUPA_URL 값:', SUPA_URL || '(없음)');
console.log('SUPA_KEY 길이:', SUPA_KEY ? SUPA_KEY.length : 0);

// 필수 환경변수 검증
if (!META_TOKEN || !META_AD_ACCOUNT || !SUPA_URL || !SUPA_KEY) {
  console.error('❌ 필수 환경변수가 누락되었습니다.');
  process.exit(1);
}

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

  // 1) Meta API 호출
  const url = `https://graph.facebook.com/v16.0/${META_AD_ACCOUNT}/insights` +
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

  // 2) 데이터 처리 및 지표 계산
  const rows = data.map(r => {
    const date = r.date_start;
    const spend = Number(r.spend);
    const impressions = Number(r.impressions);
    
    // 링크클릭수 추출 (실제 마케팅 지표)
    const linkClickAction = (r.actions || []).find(a => a.action_type === 'link_click');
    const linkClicks = linkClickAction ? Number(linkClickAction.value) : 0;

    // 구매 관련 지표 추출
    const conversionCountAction = (r.actions || []).find(a => a.action_type === 'purchase');
    const conversionValueAction = (r.action_values || []).find(a => a.action_type === 'purchase');
    
    const conversionCount = conversionCountAction ? Number(conversionCountAction.value) : 0;
    const conversionValue = conversionValueAction ? Number(conversionValueAction.value) : 0;

    // CPA 계산 (API 값 우선, fallback으로 계산)
    const cpaEntry = (r.cost_per_action_type || []).find(a => a.action_type === 'purchase');
    const cpa = cpaEntry
      ? Number(cpaEntry.value)
      : (conversionCount ? spend / conversionCount : 0);

    // 핵심 지표 계산
    const ctr = impressions ? linkClicks / impressions : 0;
    const cpc = linkClicks ? spend / linkClicks : 0;
    const cvr = linkClicks ? conversionCount / linkClicks : 0;
    const cpm = impressions ? (spend / impressions) * 1000 : 0;
    const roas = spend ? (conversionValue / spend) : 0;
    const aov = conversionCount ? (conversionValue / conversionCount) : 0;

    return {
      date,
      campaign: 'Meta',
      spend,
      impressions,
      clicks: linkClicks,
      ctr,
      cpc,
      conversion: conversionCount,
      conversion_value: conversionValue,
      roas,
      cvr,
      cpm,
      cpa,
      aov,
    };
  });

  console.log(`📝 처리된 데이터 (${rows.length}건):`, rows);

  // 3) Supabase에 데이터 저장
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