from typing import Tuple

from notion_client import Client

from app.core.config import settings


class NotionUtils:
    def __init__(self):
        self.notion = Client(auth=settings.NOTION_SECRET_KEY)

    def link_parser(self, link: str) -> str:
        """공지사항 링크 파싱"""
        # URL에서 마지막 부분 추출
        link = link.split("/")[-1]
        # 쿼리 파라미터 제거
        link = link.split("?")[0]
        # 날짜 접두사 제거 (YYYY-MM-DD- 형식)
        if "-" in link:
            link = link.split("-", 3)[-1]
        return link

    def get_notion_content(self, link: str) -> Tuple[str, str]:
        """공지사항 상세 조회"""
        # 링크 파싱
        page_id = self.link_parser(link)

        # 페이지 메타데이터 조회
        page = self.notion.pages.retrieve(page_id=page_id)
        print(f"page: \n{page}")

        # 제목 추출
        title = ""
        for prop in page.get("properties", {}).values():
            if prop.get("type") == "title":
                title = self.rich_text_to_md(prop.get("title", []))
                break

        # 블록 조회
        blocks = []
        has_more = True
        start_cursor = None

        while has_more:
            response = self.notion.blocks.children.list(block_id=page_id, start_cursor=start_cursor, page_size=100)
            blocks.extend(response.get("results", []))
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")
        print(f"blocks: \n{blocks}")
        # 마크다운으로 변환
        content = self.notion_blocks_to_markdown(blocks)
        print(f"content: \n{content}")
        return title, content

    def rich_text_to_md(self, rich_text_list):
        """Rich text를 마크다운으로 변환"""
        md = ""
        for rt in rich_text_list:
            text = rt.get("plain_text", "")
            ann = rt.get("annotations", {})

            # Apply text decorations
            if ann.get("code"):
                text = f"`{text}`"
            if ann.get("bold"):
                text = f"**{text}**"
            if ann.get("italic"):
                text = f"*{text}*"
            if ann.get("strikethrough"):
                text = f"~~{text}~~"
            if ann.get("underline"):
                text = f"<u>{text}</u>"

            # Handle links
            if rt.get("href"):
                text = f"[{text}]({rt['href']})"

            # Handle color (if needed)
            if ann.get("color") != "default":
                text = f"<span style='color:{ann['color']}'>{text}</span>"

            md += text
        return md

    def block_to_md(self, block):
        """Notion 블록을 마크다운으로 변환"""
        block_type = block["type"]
        content = block[block_type]

        if block_type == "paragraph":
            text = self.rich_text_to_md(content.get("rich_text", []))
            return f"{text}\n\n" if text else "\n"

        elif block_type.startswith("heading_"):
            level = int(block_type[-1])
            text = self.rich_text_to_md(content.get("rich_text", []))
            return f"{'#' * level} {text}\n\n"

        elif block_type == "bulleted_list_item":
            text = self.rich_text_to_md(content.get("rich_text", []))
            return f"- {text}\n"

        elif block_type == "numbered_list_item":
            text = self.rich_text_to_md(content.get("rich_text", []))
            return f"1. {text}\n"

        elif block_type == "to_do":
            checked = content.get("checked", False)
            text = self.rich_text_to_md(content.get("rich_text", []))
            return f"- [{'x' if checked else ' '}] {text}\n"

        elif block_type == "toggle":
            text = self.rich_text_to_md(content.get("rich_text", []))
            return f"<details>\n<summary>{text}</summary>\n\n"

        elif block_type == "code":
            language = content.get("language", "")
            text = content.get("rich_text", [])[0].get("plain_text", "") if content.get("rich_text") else ""
            return f"```{language}\n{text}\n```\n\n"

        elif block_type == "image":
            image_type = content.get("type")
            if image_type == "external":
                url = content.get("external", {}).get("url", "")
            else:
                url = content.get("file", {}).get("url", "")
                # S3 URL에서 파일명만 추출
                if "?" in url:
                    url = url.split("?")[0]
                if "/" in url:
                    url = url.split("/")[-1]
            caption = self.rich_text_to_md(content.get("caption", []))
            return f"![{caption}]({url})\n\n"

        elif block_type == "divider":
            return "---\n\n"

        elif block_type == "quote":
            text = self.rich_text_to_md(content.get("rich_text", []))
            return f"> {text}\n"

        elif block_type == "callout":
            icon = content.get("icon", {}).get("emoji", "💡")
            text = self.rich_text_to_md(content.get("rich_text", []))
            return f"{icon} {text}\n\n"

        elif block_type == "equation":
            expression = content.get("expression", "")
            return f"$$\n{expression}\n$$\n\n"

        elif block_type == "table":
            # Table handling is complex and requires processing multiple blocks
            return "[Table content]\n\n"

        elif block_type == "column_list":
            return "\n"

        elif block_type == "column":
            return ""

        else:
            return f"[Unsupported block type: {block_type}]\n\n"

    def notion_blocks_to_markdown(self, blocks: list) -> str:
        """Notion 블록 리스트를 마크다운으로 변환"""
        markdown = ""
        in_toggle = False
        in_quote = False
        quote_content = []

        for block in blocks:
            block_type = block["type"]

            # Handle toggle block closing
            if in_toggle and block_type != "toggle":
                markdown += "</details>\n\n"
                in_toggle = False

            # Handle toggle block opening
            if block_type == "toggle":
                in_toggle = True

            # Handle quote block formatting
            if block_type == "quote":
                if not in_quote:
                    in_quote = True
                    quote_content = []
                quote_content.append(self.block_to_md(block).strip())
            else:
                if in_quote:
                    # Process accumulated quote content
                    if quote_content:
                        # Add quote markers to each line
                        quoted_lines = []
                        for line in "\n".join(quote_content).split("\n"):
                            if line.strip():
                                if line.startswith("- "):  # Handle bullet points in quotes
                                    quoted_lines.append(f"> {line}")
                                else:
                                    quoted_lines.append(f"> {line}")
                            else:
                                quoted_lines.append(">")
                        markdown += "\n".join(quoted_lines) + "\n\n"
                    in_quote = False
                    quote_content = []
                markdown += self.block_to_md(block)

        # Close any open blocks
        if in_toggle:
            markdown += "</details>\n\n"

        if in_quote and quote_content:
            # Process any remaining quote content
            quoted_lines = []
            for line in "\n".join(quote_content).split("\n"):
                if line.strip():
                    if line.startswith("- "):  # Handle bullet points in quotes
                        quoted_lines.append(f"> {line}")
                    else:
                        quoted_lines.append(f"> {line}")
                else:
                    quoted_lines.append(">")
            markdown += "\n".join(quoted_lines) + "\n\n"

        # Remove excessive newlines while preserving paragraph structure
        lines = markdown.split("\n")
        cleaned_lines = []
        prev_empty = False

        for line in lines:
            is_empty = not line.strip()
            if not (is_empty and prev_empty):  # Don't add consecutive empty lines
                cleaned_lines.append(line)
            prev_empty = is_empty

        return "\n".join(cleaned_lines).strip()

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
