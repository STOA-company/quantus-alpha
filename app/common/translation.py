from typing import List, Optional
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


async def get_translation(
    session: AsyncSession, translation_model: any, model_id: int, language_code: str, foreign_key_column: str
) -> Optional[any]:
    """
    모델의 번역 정보를 조회하는 함수입니다.

    Args:
        session: DB 세션
        translation_model: 번역 모델 클래스 (예: BranchTranslation)
        model_id: 원본 모델의 ID
        language_code: 요청된 언어 코드 (예: "ko", "en")
        foreign_key_column: 번역 테이블에서 사용하는 외래키 컬럼명

    Returns:
        번역 데이터 또는 None
    """

    # 번역 데이터 조회
    translation_query = select(translation_model).where(
        getattr(translation_model, foreign_key_column) == model_id, translation_model.language_code == language_code
    )
    translation_result = await session.execute(translation_query)
    return translation_result.scalar_one_or_none()


async def get_translations_for_list(
    session: AsyncSession, model_ids: List[int], translation_model: any, language_code: str, foreign_key_column: str
):
    """
    모델의 번역 정보를 조회하는 함수입니다.

    Args:
        session: DB 세션
        model_ids: 번역이 필요한 모델의 ID 리스트
        translation_model: 번역 모델 클래스 (예: EducationTranslation)
        language_code: 요청된 언어 코드
        foreign_key_column: 번역 테이블의 외래키 컬럼명

    Returns:
        번역 데이터 리스트
    """
    if not model_ids:
        return []

    # 번역 데이터 일괄 조회
    translation_query = select(translation_model).where(
        getattr(translation_model, foreign_key_column).in_(model_ids), translation_model.language_code == language_code
    )
    translation_result = await session.execute(translation_query)
    translations = translation_result.scalars().all()

    # ID 순서대로 번역 데이터 반환
    translations_dict = {getattr(trans, foreign_key_column): trans for trans in translations}
    return [translations_dict.get(id) for id in model_ids]


def korean_to_english_typing(text: str) -> str:
    """
    한글을 영타로 변환하는 함수
    """
    k_to_e = {
        # 초성
        "ㄱ": "r",
        "ㄲ": "R",
        "ㄴ": "s",
        "ㄷ": "e",
        "ㄸ": "E",
        "ㄹ": "f",
        "ㅁ": "a",
        "ㅂ": "q",
        "ㅃ": "Q",
        "ㅅ": "t",
        "ㅆ": "T",
        "ㅇ": "d",
        "ㅈ": "w",
        "ㅉ": "W",
        "ㅊ": "c",
        "ㅋ": "z",
        "ㅌ": "x",
        "ㅍ": "v",
        "ㅎ": "g",
        # 중성
        "ㅏ": "k",
        "ㅐ": "o",
        "ㅑ": "i",
        "ㅒ": "O",
        "ㅓ": "j",
        "ㅔ": "p",
        "ㅕ": "u",
        "ㅖ": "P",
        "ㅗ": "h",
        "ㅘ": "hk",
        "ㅙ": "ho",
        "ㅚ": "hl",
        "ㅛ": "y",
        "ㅜ": "n",
        "ㅝ": "nj",
        "ㅞ": "np",
        "ㅟ": "nl",
        "ㅠ": "b",
        "ㅡ": "m",
        "ㅢ": "ml",
        "ㅣ": "l",
        # 종성
        "ㄳ": "rt",
        "ㄵ": "sw",
        "ㄶ": "sg",
        "ㄺ": "fr",
        "ㄻ": "fa",
        "ㄼ": "fq",
        "ㄽ": "ft",
        "ㄾ": "fx",
        "ㄿ": "fv",
        "ㅀ": "fg",
        "ㅄ": "qt",
    }
    # 초성 리스트
    CHOSUNG = [
        "ㄱ",
        "ㄲ",
        "ㄴ",
        "ㄷ",
        "ㄸ",
        "ㄹ",
        "ㅁ",
        "ㅂ",
        "ㅃ",
        "ㅅ",
        "ㅆ",
        "ㅇ",
        "ㅈ",
        "ㅉ",
        "ㅊ",
        "ㅋ",
        "ㅌ",
        "ㅍ",
        "ㅎ",
    ]
    # 중성 리스트
    JUNGSUNG = [
        "ㅏ",
        "ㅐ",
        "ㅑ",
        "ㅒ",
        "ㅓ",
        "ㅔ",
        "ㅕ",
        "ㅖ",
        "ㅗ",
        "ㅘ",
        "ㅙ",
        "ㅚ",
        "ㅛ",
        "ㅜ",
        "ㅝ",
        "ㅞ",
        "ㅟ",
        "ㅠ",
        "ㅡ",
        "ㅢ",
        "ㅣ",
    ]
    # 종성 리스트
    JONGSUNG = [
        "",
        "ㄱ",
        "ㄲ",
        "ㄳ",
        "ㄴ",
        "ㄵ",
        "ㄶ",
        "ㄷ",
        "ㄹ",
        "ㄺ",
        "ㄻ",
        "ㄼ",
        "ㄽ",
        "ㄾ",
        "ㄿ",
        "ㅀ",
        "ㅁ",
        "ㅂ",
        "ㅄ",
        "ㅅ",
        "ㅆ",
        "ㅇ",
        "ㅈ",
        "ㅊ",
        "ㅋ",
        "ㅌ",
        "ㅍ",
        "ㅎ",
    ]

    result = ""
    for char in text:
        if "가" <= char <= "힣":
            # 한글 유니코드 값에서 초성/중성/종성 분리
            char_code = ord(char) - ord("가")
            cho = char_code // (21 * 28)
            jung = (char_code % (21 * 28)) // 28
            jong = char_code % 28

            # 영타로 변환
            result += k_to_e[CHOSUNG[cho]]
            result += k_to_e[JUNGSUNG[jung]]
            if jong > 0:
                result += k_to_e[JONGSUNG[jong]]
        else:
            result += char
    return result
