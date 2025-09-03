#!/usr/bin/env python3
"""
Markdown to PDF 변환 테스트 (Pandoc 기반)
"""

import sys
import os
import tempfile

# 프로젝트 루트 디렉토리를 Python 경로에 추가
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from app.utils.markdown_to_pdf_utils import markdown_to_pdf, create_github_style_pdf, create_simple_pdf

def test_simple_markdown_to_pdf():
    """간단한 마크다운을 PDF로 변환하는 테스트"""
    print("📄 간단한 마크다운 to PDF 테스트 (Pandoc)")
    
    # 테스트용 마크다운 내용
    simple_markdown = """
# 테스트 문서

## 소개
이것은 테스트용 마크다운 문서입니다.

### 기능
- 마크다운을 HTML로 변환
- HTML을 PDF로 변환
- 스타일링 적용

## 코드 예시
```python
def hello_world():
    print("Hello, World!")
```

## 테이블 예시
| 항목 | 설명 |
|------|------|
| 기능1 | 마크다운 변환 |
| 기능2 | PDF 생성 |
| 기능3 | 스타일링 |

---
*테스트 완료*
"""
    
    try:
        # 현재 디렉토리에 PDF 파일 생성
        pdf_path = "simple_test.pdf"
        
        # 마크다운을 PDF로 변환
        result = markdown_to_pdf(simple_markdown, pdf_path)
        
        if result and os.path.exists(pdf_path):
            file_size = os.path.getsize(pdf_path)
            print(f"✅ PDF 생성 성공! 파일 크기: {file_size} bytes")
            print(f"📁 PDF 파일 위치: {pdf_path}")
        else:
            print("❌ PDF 파일이 생성되지 않았습니다.")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def test_github_style_pdf():
    """GitHub 스타일 마크다운을 PDF로 변환하는 테스트"""
    print("\n📄 GitHub 스타일 마크다운 to PDF 테스트")
    
    # 테스트용 마크다운 내용
    github_markdown = """
# 엔비디아 미래 전망 종합 보고서\n\n## 핵심 요약\n- 엔비디아는 인공지능, 데이터 센터, 자율주행, 메타버스 등 핵심 산업에서 지속적인 기술 혁신과 전략적 확장을 추진하며 시장 기대를 높이고 있음.\n- 최근 뉴스와 공개자료는 엔비디아의 글로벌 협력 강화, 차세대 GPU 개발, AI 생태계 확장에 집중하는 전략을 보여줌.\n- 재무적으로도 1년간 주가가 약 46% 상승하며 강력한 성장 모멘텀을 유지하고 있으며, 수익성도 높게 기록되고 있음.\n- 시장 동향은 글로벌 수요 증가와 AI/반도체 시장의 성장에 힘입어 장기 성장 잠재력이 매우 높음.\n- 경쟁사 대비 기술적 우위와 시장 점유력을 확보하며, 글로벌 협력 및 신사업 다변화 전략으로 지속 성장이 기대됨.\n- 다만, 글로벌 공급망 문제와 지정학적 리스크는 잠재적 리스크로 남아 있으나, 현재 자료들은 엔비디아의 장기 성장 가능성을 높게 평가하는 방향임.\n\n## 상세 분석\n\n### 1. 시장 및 업계 동향\n- 글로벌 AI 및 반도체 시장은 연평균 성장률이 높으며, 엔비디아는 AI 하드웨어와 데이터 센터 시장에서 선도적 위치를 유지하고 있음.\n- 최근 수집한 뉴스와 공개자료에 따르면, 엔비디아는 차세대 GPU 개발, 글로벌 협력 확대, 친환경 기술 도입 등에 적극 투자하고 있으며, 인공지능 생태계 확장 전략을 추진 중.\n- 글로벌 협력 사례로는 한국, 미국, 유럽 등에서 표준화와 기술 개발 협력 강화를 보여주고 있음.\n\n### 2. 재무 상태와 주가 추세\n- 최근 1년간 주가가 약 46% 상승하며 시장의 신뢰를 받고 있으며, 매출과 영업이익 모두 역대 최고치를 기록.\n- 실적 발표에 따르면, 데이터 센터와 AI 관련 사업이 수익성 향상에 크게 기여.\n- 글로벌 시장에서 AI와 반도체 수요 증가에 힘입어, 장기 성장 기대감이 크며, 재무 안정성도 확보된 상태임.\n\n### 3. 기술 혁신 및 전략\n- 최신 GPU 아키텍처와 AI 칩 개발에 박차를 가하며, 자율주행과 메타버스 등 신사업 분야에 집중.\n- 친환경 기술과 차세대 칩 설계로 경쟁우위 확보, 글로벌 파트너십 확대, 인수합병 전략으로 시장 지배력 강화.\n- 연구개발 투자도 지속적으로 확대하며, AI 초거대 모델과 차세대 클라우드 인프라 강화를 목표로 함.\n\n### 4. 시장 전망 및 경쟁력\n- 인공지능 및 반도체 시장의 지속적 성장 기대 속에서, 엔비디아는 기술적 우위와 시장 점유율 확대를 통해 장기 성장 모멘텀 유지.\n- 경쟁사 대비 기술력, 시장 지배력, 글로벌 협력 네트워크 강화를 통해 글로벌 시장 내 입지 강화.\n- 시장 전문가들은 엔비디아의 미래 성장 잠재력을 매우 높게 평가하며, 장기적 투자 가치가 크다고 보고 있음.\n\n### 5. 리스크 및 한계\n- 글로벌 공급망 불안과 지정학적 리스크는 잠재적 장애요인.\n- 일부 최신 뉴스와 경쟁사 동향 분석이 아직 부족한 점은 보완이 필요.\n- 환율 변동, 규제 강화 등 외부 변수도 고려해야 함.\n\n## 결론 및 추천\n엔비디아는 기술 혁신, 재무 안정성, 글로벌 협력 강화 등 다양한 강점을 바탕으로 향후 수년간 지속 성장 가능성이 매우 높음. AI와 반도체 시장의 성장세에 힘입어, 장기적 투자 관점에서 매우 유망한 기업으로 평가됨. 다만, 글로벌 공급망 문제와 경쟁 심화에 대비한 리스크 관리가 필요하며, 시장 동향을 지속 모니터링하는 전략이 중요함.\n\n---\n\n이상으로 엔비디아의 미래 전망에 대한 데이터 기반 종합 보고를 마칩니다.
"""
    
    try:
        # 현재 디렉토리에 PDF 파일 생성
        pdf_path = "github_style_test.pdf"
        
        # GitHub 스타일 마크다운을 PDF로 변환
        result = create_github_style_pdf(github_markdown, pdf_path, title="엔비디아 보고서")
        
        if result and os.path.exists(pdf_path):
            file_size = os.path.getsize(pdf_path)
            print(f"✅ GitHub 스타일 PDF 생성 성공! 파일 크기: {file_size} bytes")
            print(f"📁 PDF 파일 위치: {pdf_path}")
        else:
            print("❌ PDF 파일이 생성되지 않았습니다.")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def test_markdown_with_tables():
    """테이블이 포함된 마크다운을 PDF로 변환하는 테스트"""
    print("\n📄 테이블 포함 마크다운 to PDF 테스트")
    
    table_markdown = """
# 포트폴리오 성과 보고서

## 월별 수익률

| 월 | 수익률 | 벤치마크 | 초과수익률 |
|----|--------|----------|------------|
| 1월 | 2.5% | 1.8% | 0.7% |
| 2월 | -1.2% | -0.8% | -0.4% |
| 3월 | 3.1% | 2.2% | 0.9% |
| 4월 | 1.8% | 1.5% | 0.3% |

## 자산 배분

| 자산군 | 비중 | 목표비중 | 편차 |
|--------|------|----------|------|
| 주식 | 60% | 55% | +5% |
| 채권 | 30% | 35% | -5% |
| 현금 | 10% | 10% | 0% |

## 성과 요약
- **총 수익률**: 6.2%
- **변동성**: 12.5%
- **샤프 비율**: 0.85
- **최대 낙폭**: -8.3%

---
*보고서 생성일: 2024년 12월*
"""
    
    try:
        # 현재 디렉토리에 PDF 파일 생성
        pdf_path = "table_test.pdf"
        
        # 마크다운을 PDF로 변환
        result = markdown_to_pdf(table_markdown, pdf_path)
        
        if result and os.path.exists(pdf_path):
            file_size = os.path.getsize(pdf_path)
            print(f"✅ 테이블 PDF 생성 성공! 파일 크기: {file_size} bytes")
            print(f"📁 PDF 파일 위치: {pdf_path}")
        else:
            print("❌ PDF 파일이 생성되지 않았습니다.")
            
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def test_pandoc_availability():
    """Pandoc 사용 가능 여부 확인"""
    print("\n🔍 Pandoc 사용 가능 여부 확인")
    
    try:
        import pypandoc
        version = pypandoc.get_pandoc_version()
        print(f"✅ Pandoc 버전: {version}")
        return True
    except Exception as e:
        print(f"❌ Pandoc 사용 불가: {e}")
        print("💡 Pandoc 설치가 필요합니다: https://pandoc.org/installing.html")
        return False

if __name__ == '__main__':
    print("🚀 Markdown to PDF 변환 테스트를 시작합니다... (Pandoc 기반)")
    print("=" * 60)
    
    # Pandoc 사용 가능 여부 확인
    if not test_pandoc_availability():
        print("\n❌ Pandoc이 설치되지 않아 테스트를 중단합니다.")
        sys.exit(1)
    
    # 사용자 확인
    response = input("\n테스트를 실행하시겠습니까? (y/n): ")
    if response.lower() != 'y':
        print("테스트를 취소했습니다.")
        sys.exit(0)
    
    # 테스트 실행
    test_simple_markdown_to_pdf()
    test_github_style_pdf()
    test_markdown_with_tables()
    
    print("\n" + "=" * 60)
    print("🎉 모든 테스트가 완료되었습니다!")
    print("생성된 PDF 파일들을 확인해보세요.")