DOCUMENT_TYPE_MAPPING = {  #
    # 수시공시
    "8-K": "주요 이벤트 공시",
    "6-K": "외국기업 수시공시",
    # 지분 및 인수 관련
    "SC 13D": "5% 이상 지분 취득 (적극 투자)",
    "SC 13G": "5% 이상 지분 취득 (단순 투자)",
    "SC 14D1": "공개매수 신고",
    "SC 13E3": "비공개기업 전환 공시",
    "SC 13E4": "자사주 매입 공시",
    # 정기보고서
    "10-Q": "분기 실적 보고서",
    "13F-E": "기관투자 포트폴리오 보고서",
    "10-K": "연간 실적 보고서",
    "20-F": "외국기업 연간 실적 보고서",
    "11-K": "직원 주식매수제도 보고서",
    # 증권 등록
    "S-1": "신규 증권 등록 신청",
    "S-3": "간소화 증권 등록 신청",
    "424B": "증권신고서 수정본",
    "425": "합병 및 인수 공시",
    # 위험신호
    "NT 10-K": "연간보고서 지연 공시",
    "NT 10-Q": "분기보고서 지연 공시",
    # 주주총회/의결권
    "DEF14A": "주주총회 소집 통지",
    "DEFA14A": "추가 위임장 공시",
    "DEFM14A": "합병 관련 위임장 공시",
}

DOCUMENT_TYPE_MAPPING_EN = {
    # 수시공시
    "8-K": "Current Report",
    "6-K": "Report of Foreign Private Issuer",
    # 지분 및 인수 관련
    "SC 13D": "Beneficial Ownership Report (Active)",
    "SC 13G": "Beneficial Ownership Report (Passive)",
    "SC 14D1": "Tender Offer Statement",
    "SC 13E3": "Going-Private Transaction Statement",
    "SC 13E4": "Issuer Tender Offer Statement",
    # 정기보고서
    "10-Q": "Quarterly Report",
    "13F-E": "Institutional Investment Manager Holdings Report",
    "10-K": "Annual Report",
    "20-F": "Annual Report by Foreign Private Issuer",
    "11-K": "Annual Report of Employee Stock Purchase, Savings and Similar Plans",
    # 증권 등록
    "S-1": "Registration Statement",
    "S-3": "Registration Statement (Simplified)",
    "424B": "Prospectus Filed Pursuant to Rule 424(b)",
    "425": "Filing under Rule 425 (Business Combination)",
    # 위험신호
    "NT 10-K": "Notification of Late Filing (Annual)",
    "NT 10-Q": "Notification of Late Filing (Quarterly)",
    # 주주총회/의결권
    "DEF14A": "Definitive Proxy Statement",
    "DEFA14A": "Additional Definitive Proxy Materials",
    "DEFM14A": "Definitive Proxy Statement for Merger",
}

CATEGORY_TYPE_MAPPING_EN = {
    "기재정정": "Amended Filing",  # 기존 제출 내용(기재사항)을 수정/정정하는 경우
    "발행조건확정": "Issuance Terms Finalized",  # 채권·주식 등 발행 시 최종 조건이 확정됨
    "연장결정": "Extension Determined",  # 만기·기한 등 기존 일정을 연장하기로 결정
    "첨부정정": "Amended Attachment",  # 첨부 서류(첨부파일) 내용을 정정 또는 수정
}
