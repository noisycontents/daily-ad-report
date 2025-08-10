// scripts/fetch-all-platforms.js
// 모든 광고 플랫폼 데이터 통합 수집 스크립트

import { fetchMetaData } from './fetch-meta.js';
import { fetchNaverData } from './fetch-naver.js';
import { fetchGoogleData } from './fetch-google.js';
import { fetchMetaData as fetchDokMetaData } from './fetch-meta-dok.js';
import { fetchNaverData as fetchDokNaverData } from './fetch-naver-dok.js';
import { fetchGoogleData as fetchDokGoogleData } from './fetch-google-dok.js';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';

// 환경변수 로드
dotenv.config();

const supa = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

async function fetchAllPlatforms() {
  const startTime = Date.now();
  const today = new Date().toISOString().slice(0, 10);
  
  console.log(`🚀 모든 광고 플랫폼 데이터 수집 시작 (${today})`);
  console.log('='.repeat(60));

  const results = {
    meta: { success: false, error: null, count: 0 },
    naver: { success: false, error: null, count: 0 },
    google: { success: false, error: null, count: 0 },
    dok_meta: { success: false, error: null, count: 0 },
    dok_naver: { success: false, error: null, count: 0 },
    dok_google: { success: false, error: null, count: 0 }
  };

  // 1) Meta 광고 데이터 수집
  console.log('\n🔵 Meta (Facebook) 광고 데이터 수집...');
  try {
    await fetchMetaData();
    results.meta.success = true;
    console.log('✅ Meta 데이터 수집 완료');
  } catch (error) {
    results.meta.error = error.message;
    console.error('❌ Meta 데이터 수집 실패:', error.message);
  }

  // 2) 네이버 광고 데이터 수집
  console.log('\n🟢 네이버 광고 데이터 수집...');
  try {
    await fetchNaverData();
    results.naver.success = true;
    console.log('✅ 네이버 데이터 수집 완료');
  } catch (error) {
    results.naver.error = error.message;
    console.error('❌ 네이버 데이터 수집 실패:', error.message);
  }

  // 3) 구글 광고 데이터 수집
  console.log('\n🔴 구글 광고 데이터 수집...');
  try {
    await fetchGoogleData();
    results.google.success = true;
    console.log('✅ 구글 데이터 수집 완료');
  } catch (error) {
    results.google.error = error.message;
    console.error('❌ 구글 데이터 수집 실패:', error.message);
  }

  // 4) DOK Meta 광고 데이터 수집
  console.log('\n🔵 DOK Meta (Facebook) 광고 데이터 수집...');
  try {
    await fetchDokMetaData();
    results.dok_meta.success = true;
    console.log('✅ DOK Meta 데이터 수집 완료');
  } catch (error) {
    results.dok_meta.error = error.message;
    console.error('❌ DOK Meta 데이터 수집 실패:', error.message);
  }

  // 5) DOK 네이버 광고 데이터 수집
  console.log('\n🟢 DOK 네이버 광고 데이터 수집...');
  try {
    await fetchDokNaverData();
    results.dok_naver.success = true;
    console.log('✅ DOK 네이버 데이터 수집 완료');
  } catch (error) {
    results.dok_naver.error = error.message;
    console.error('❌ DOK 네이버 데이터 수집 실패:', error.message);
  }

  // 6) DOK 구글 광고 데이터 수집
  console.log('\n🔴 DOK 구글 광고 데이터 수집...');
  try {
    await fetchDokGoogleData();
    results.dok_google.success = true;
    console.log('✅ DOK 구글 데이터 수집 완료');
  } catch (error) {
    results.dok_google.error = error.message;
    console.error('❌ DOK 구글 데이터 수집 실패:', error.message);
  }

  // 7) 결과 통계 조회
  console.log('\n📊 수집 결과 통계...');
  try {
    const stats = await getCollectionStats(today);
    
    // 플랫폼별 데이터 건수 업데이트
    results.meta.count = stats.meta || 0;
    results.naver.count = stats.naver || 0;
    results.google.count = stats.google || 0;
    results.dok_meta.count = stats.dok_meta || 0;
    results.dok_naver.count = stats.dok_naver || 0;
    results.dok_google.count = stats.dok_google || 0;

    displaySummary(results, stats, startTime);
  } catch (error) {
    console.error('❌ 통계 조회 실패:', error.message);
  }

  // 5) 최종 결과
  const successCount = Object.values(results).filter(r => r.success).length;
  const totalPlatforms = Object.keys(results).length;

  console.log('\n🏁 최종 결과:');
  if (successCount === totalPlatforms) {
    console.log('🎉 모든 플랫폼 데이터 수집 성공!');
  } else if (successCount > 0) {
    console.log(`⚠️ 부분 성공: ${successCount}/${totalPlatforms} 플랫폼 완료`);
  } else {
    console.log('💥 모든 플랫폼 데이터 수집 실패');
    process.exit(1);
  }
}

// 수집 통계 조회 (각 테이블별로 개별 조회)
async function getCollectionStats(date) {
  try {
    // 각 플랫폼별 테이블에서 데이터 건수 조회 (기존 + DOK)
    const [metaResult, naverResult, googleResult, dokMetaResult, dokNaverResult, dokGoogleResult] = await Promise.all([
      supa.from('meta_insights').select('id', { count: 'exact' }).eq('date', date),
      supa.from('naver_insights').select('id', { count: 'exact' }).eq('date', date),
      supa.from('google_insights').select('id', { count: 'exact' }).eq('date', date),
      supa.from('dok_meta_insights').select('id', { count: 'exact' }).eq('date', date),
      supa.from('dok_naver_insights').select('id', { count: 'exact' }).eq('date', date),
      supa.from('dok_google_insights').select('id', { count: 'exact' }).eq('date', date)
    ]);

    const stats = {
      meta: metaResult.count || 0,
      naver: naverResult.count || 0,
      google: googleResult.count || 0,
      dok_meta: dokMetaResult.count || 0,
      dok_naver: dokNaverResult.count || 0,
      dok_google: dokGoogleResult.count || 0,
      original_total: (metaResult.count || 0) + (naverResult.count || 0) + (googleResult.count || 0),
      dok_total: (dokMetaResult.count || 0) + (dokNaverResult.count || 0) + (dokGoogleResult.count || 0),
      grand_total: (metaResult.count || 0) + (naverResult.count || 0) + (googleResult.count || 0) + 
                   (dokMetaResult.count || 0) + (dokNaverResult.count || 0) + (dokGoogleResult.count || 0)
    };

    return stats;
  } catch (error) {
    console.error('통계 조회 에러:', error);
    return { meta: 0, naver: 0, google: 0, dok_meta: 0, dok_naver: 0, dok_google: 0, original_total: 0, dok_total: 0, grand_total: 0 };
  }
}

// 결과 요약 표시
function displaySummary(results, stats, startTime) {
  const duration = ((Date.now() - startTime) / 1000).toFixed(1);
  
  console.log('\n📈 수집 결과 요약');
  console.log('┌─────────────┬─────────┬─────────┬──────────────────┐');
  console.log('│ 플랫폼      │ 상태    │ 데이터  │ 에러             │');
  console.log('├─────────────┼─────────┼─────────┼──────────────────┤');
  
  // Meta
  const metaStatus = results.meta.success ? '✅ 성공' : '❌ 실패';
  const metaError = results.meta.error ? results.meta.error.substring(0, 16) : '';
  console.log(`│ Meta        │ ${metaStatus} │ ${results.meta.count.toString().padStart(7)} │ ${metaError.padEnd(16)} │`);
  
  // 네이버
  const naverStatus = results.naver.success ? '✅ 성공' : '❌ 실패';
  const naverError = results.naver.error ? results.naver.error.substring(0, 16) : '';
  console.log(`│ 네이버      │ ${naverStatus} │ ${results.naver.count.toString().padStart(7)} │ ${naverError.padEnd(16)} │`);
  
  // 구글
  const googleStatus = results.google.success ? '✅ 성공' : '❌ 실패';
  const googleError = results.google.error ? results.google.error.substring(0, 16) : '';
  console.log(`│ 구글        │ ${googleStatus} │ ${results.google.count.toString().padStart(7)} │ ${googleError.padEnd(16)} │`);
  
  // DOK Meta
  const dokMetaStatus = results.dok_meta.success ? '✅ 성공' : '❌ 실패';
  const dokMetaError = results.dok_meta.error ? results.dok_meta.error.substring(0, 16) : '';
  console.log(`│ DOK Meta    │ ${dokMetaStatus} │ ${results.dok_meta.count.toString().padStart(7)} │ ${dokMetaError.padEnd(16)} │`);
  
  // DOK 네이버
  const dokNaverStatus = results.dok_naver.success ? '✅ 성공' : '❌ 실패';
  const dokNaverError = results.dok_naver.error ? results.dok_naver.error.substring(0, 16) : '';
  console.log(`│ DOK 네이버  │ ${dokNaverStatus} │ ${results.dok_naver.count.toString().padStart(7)} │ ${dokNaverError.padEnd(16)} │`);
  
  // DOK 구글
  const dokGoogleStatus = results.dok_google.success ? '✅ 성공' : '❌ 실패';
  const dokGoogleError = results.dok_google.error ? results.dok_google.error.substring(0, 16) : '';
  console.log(`│ DOK 구글    │ ${dokGoogleStatus} │ ${results.dok_google.count.toString().padStart(7)} │ ${dokGoogleError.padEnd(16)} │`);
  
  console.log('├─────────────┼─────────┼─────────┼──────────────────┤');
  console.log(`│ 기존 합계   │         │ ${stats.original_total.toString().padStart(7)} │                  │`);
  console.log(`│ DOK 합계    │         │ ${stats.dok_total.toString().padStart(7)} │                  │`);
  console.log('├─────────────┼─────────┼─────────┼──────────────────┤');
  console.log(`│ 전체 합계   │         │ ${stats.grand_total.toString().padStart(7)} │                  │`);
  console.log('└─────────────┴─────────┴─────────┴──────────────────┘');
  
  console.log(`⏱️ 총 소요시간: ${duration}초`);
}

// 개별 플랫폼 실행 함수들 (디버깅용)
async function runMetaOnly() {
  console.log('🔵 Meta 전용 실행 모드');
  await fetchMetaData();
}

async function runNaverOnly() {
  console.log('🟢 네이버 전용 실행 모드');
  await fetchNaverData();
}

async function runGoogleOnly() {
  console.log('🔴 구글 전용 실행 모드');
  await fetchGoogleData();
}

// 스크립트 직접 실행 시
if (import.meta.url === `file://${process.argv[1]}`) {
  const mode = process.argv[2];
  
  try {
    switch (mode) {
      case 'meta':
        await runMetaOnly();
        break;
      case 'naver':
        await runNaverOnly();
        break;
      case 'google':
        await runGoogleOnly();
        break;
      default:
        await fetchAllPlatforms();
        break;
    }
  } catch (error) {
    console.error('💥 실행 에러:', error);
    process.exit(1);
  }
}

export { fetchAllPlatforms, runMetaOnly, runNaverOnly, runGoogleOnly }; 