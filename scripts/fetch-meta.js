// scripts/fetch-meta.js

import fetch from 'node-fetch';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

// 환경변수 로드
dotenv.config();

// 환경변수 읽기
const META_TOKEN = process.env.META_TOKEN;
const META_AD_ACCOUNT = process.env.META_AD_ACCOUNT;
const SUPABASE_URL   = process.env.SUPABASE_URL;
const SUPABASE_KEY   = process.env.SUPABASE_KEY;

// 환경변수 확인
console.log('🔧 환경변수 체크:');
console.log('META_TOKEN:', META_TOKEN ? '✅ 설정됨' : '❌ 없음');
console.log('META_AD_ACCOUNT:', META_AD_ACCOUNT ? '✅ 설정됨' : '❌ 없음');
console.log('SUPABASE_URL:', SUPABASE_URL ? '✅ 설정됨' : '❌ 없음');
console.log('SUPABASE_KEY:', SUPABASE_KEY ? '✅ 설정됨' : '❌ 없음');

// 디버깅: 환경변수 값 일부 표시 (보안을 위해 일부만)
console.log('🔍 환경변수 값 확인:');
console.log('META_TOKEN 길이:', META_TOKEN ? META_TOKEN.length : 0);
console.log('META_AD_ACCOUNT 값:', META_AD_ACCOUNT || '(없음)');
console.log('SUPABASE_URL 값:', SUPABASE_URL || '(없음)');
console.log('SUPABASE_KEY 길이:', SUPABASE_KEY ? SUPABASE_KEY.length : 0);

// 필수 환경변수 검증
if (!META_TOKEN || !META_AD_ACCOUNT || !SUPABASE_URL || !SUPABASE_KEY) {
  console.error('❌ 필수 환경변수가 누락되었습니다.');
  process.exit(1);
}

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
const testDates = []; // 테스트할 날짜들 (예: ['2025-08-09'])

// 환경변수에서 TARGET_DATE 읽기 (백필 스크립트 지원)
const getTargetDate = () => {
  const envDate = process.env.TARGET_DATE;
  if (envDate && /^\d{4}-\d{2}-\d{2}$/.test(envDate)) {
    return envDate;
  }
  return null;
};

async function fetchAndUpsert() {
  // 우선순위: TARGET_DATE 환경변수 > testDates 배열 > 어제 날짜
  const envTargetDate = getTargetDate();
  const datesToRun = envTargetDate 
    ? [envTargetDate]
    : (Array.isArray(testDates) && testDates.length > 0)
    ? testDates
    : [getKSTYesterday()];
  console.log(`\n📅 Meta 데이터 수집 시작 (총 ${datesToRun.length}개 날짜)`);

  for (const targetDate of datesToRun) {
    console.log(`\n📅 처리 날짜: ${targetDate}`);

    // 1) Meta API 호출
    const url = `https://graph.facebook.com/v16.0/${META_AD_ACCOUNT}/insights` +
                `?time_range={'since':'${targetDate}','until':'${targetDate}'}` +
                `&fields=date_start,spend,impressions,clicks,actions,action_values,cost_per_action_type` +
                `&access_token=${META_TOKEN}`;

    console.log('🌐 Meta API 호출 중...');
    
    // 재시도 로직 (Rate Limit 대응)
    let responseData;
    const maxRetries = 3;
    const baseDelay = 30000; // 30초 기본 대기
    
    for (let attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        const res = await fetch(url);
        
        if (res.ok) {
          responseData = await res.json();
          if (attempt > 1) {
            console.log(`✅ Meta API 재시도 ${attempt}번째 성공!`);
          }
          break;
        }
        
        const errorText = await res.text();
        const errorData = JSON.parse(errorText);
        
        // Rate Limit 에러 확인 (code: 4, 17, 32, 613)
        const isRateLimit = [4, 17, 32, 613].includes(errorData?.error?.code) || 
                           errorData?.error?.is_transient === true;
        
        if (isRateLimit && attempt < maxRetries) {
          const waitTime = baseDelay * Math.pow(2, attempt - 1); // 지수 백오프
          console.log(`⚠️ Meta API Rate Limit (시도 ${attempt}/${maxRetries}): ${waitTime/1000}초 대기 후 재시도...`);
          console.log(`📄 에러 내용: ${errorData?.error?.error_user_msg || errorData?.error?.message}`);
          
          await new Promise(resolve => setTimeout(resolve, waitTime));
          continue;
        }
        
        // 재시도 불가능한 에러 또는 최대 시도 횟수 초과
        console.error('❌ Meta API 에러:', res.status, res.statusText);
        console.error('응답 내용:', errorText);
        throw new Error(`Meta API error: ${res.status} ${res.statusText}`);
        
      } catch (fetchError) {
        if (attempt === maxRetries) {
          throw fetchError;
        }
        console.log(`⚠️ Meta API 네트워크 에러 (시도 ${attempt}/${maxRetries}): 30초 후 재시도...`);
        await new Promise(resolve => setTimeout(resolve, 30000));
      }
    }
    console.log('📊 Meta API 응답:', responseData);
    const { data } = responseData;

    // 2) 데이터 처리 및 지표 계산
    const rows = data.map(r => {
      const date = r.date_start || targetDate;
      const spend = Number(r.spend);
      const impressions = Number(r.impressions);
      
      const linkClickAction = (r.actions || []).find(a => a.action_type === 'link_click');
      const linkClicks = linkClickAction ? Number(linkClickAction.value) : 0;

      const conversionCountAction = (r.actions || []).find(a => a.action_type === 'purchase');
      const conversionValueAction = (r.action_values || []).find(a => a.action_type === 'purchase');
      
      const conversionCount = conversionCountAction ? Number(conversionCountAction.value) : 0;
      const conversionValue = conversionValueAction ? Number(conversionValueAction.value) : 0;

      const cpaEntry = (r.cost_per_action_type || []).find(a => a.action_type === 'purchase');
      const cpa = cpaEntry
        ? Number(cpaEntry.value)
        : (conversionCount ? spend / conversionCount : 0);

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
    console.log(`✅ ${targetDate} 데이터 ${rows.length}건 upsert 완료`);
  }
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