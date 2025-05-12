from typing import Tuple

from notion_client import Client

from app.core.config import settings


class NotionUtils:
    def __init__(self):
        self.notion = Client(auth=settings.NOTION_SECRET_KEY)

    def link_parser(self, link: str) -> str:
        """공지사항 링크 파싱"""
        link = link.split("/")[-1]
        link = link.split("?")[0]
        # link = link.split("#")[0]
        # link = link.split(".")[0]
        # link = link.split("-")[0]
        return link

    def get_notion_content(self, link: str) -> Tuple[str, str]:
        """공지사항 상세 조회"""
        # 링크 파싱
        link = self.link_parser(link)

        # 공지사항 상세 조회
        response = self.notion.blocks.retrieve(block_id=link)

        return response

    def rich_text_to_md(self, rich_text_list):
        md = ""
        for rt in rich_text_list:
            text = rt.get("plain_text", "")
            ann = rt.get("annotations", {})
            if ann.get("code"):
                text = f"`{text}`"
            if ann.get("bold"):
                text = f"**{text}**"
            if ann.get("italic"):
                text = f"*{text}*"
            if ann.get("strikethrough"):
                text = f"~~{text}~~"
            if ann.get("underline"):
                text = f"<u>{text}</u>"  # Markdown에 underline 없음
            if rt.get("href"):
                text = f"[{text}]({rt['href']})"
            md += text
        return md

    def block_to_md(self, block):
        block_type = block["type"]
        content = block[block_type]

        if block_type == "paragraph":
            return self.rich_text_to_md(content.get("rich_text", [])) + "\n"
        elif block_type.startswith("heading_"):
            level = block_type[-1]
            return f"{'#' * int(level)} {self.rich_text_to_md(content.get('rich_text', []))}\n"
        elif block_type == "image":
            image_url = content.get("file", {}).get("url") or content.get("external", {}).get("url")
            return f"![image]({image_url})\n"
        elif block_type == "divider":
            return "---\n"
        elif block_type == "numbered_list_item":
            return f"1. {self.rich_text_to_md(content.get('rich_text', []))}\n"
        elif block_type == "bulleted_list_item":
            return f"- {self.rich_text_to_md(content.get('rich_text', []))}\n"
        elif block_type == "quote":
            return "> " + self.rich_text_to_md(content.get("rich_text", [])) + "\n"
        else:
            return f"[Unsupported block: {block_type}]\n"

    def notion_blocks_to_markdown(self, blocks: list) -> str:
        return "\n".join([self.block_to_md(block) for block in blocks])

    def get_notion_title_and_content(self, link: str):
        """노션 링크에서 제목과 마크다운 변환된 본문을 반환"""
        # 링크 파싱
        page_id = self.link_parser(link)
        # 페이지 메타데이터
        page = self.notion.pages.retrieve(page_id=page_id)
        # 제목 추출
        title = ""
        # Notion DB 구조에 따라 title property가 다를 수 있음
        for prop in page.get("properties", {}).values():
            if prop.get("type") == "title":
                title = self.rich_text_to_md(prop.get("title", []))
                break
        # 블록(본문) 추출
        blocks = self.notion.blocks.children.list(block_id=page_id)
        content = self.notion_blocks_to_markdown(blocks.get("results", []))
        return title, content
