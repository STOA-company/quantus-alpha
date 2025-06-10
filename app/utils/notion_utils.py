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
        title = self.notion_2_md_title(json_notion=page)

        # 블록 조회
        blocks = []
        has_more = True
        start_cursor = None

        while has_more:
            response = self.notion.blocks.children.list(block_id=page_id, start_cursor=start_cursor, page_size=100)
            blocks.extend(response.get("results", []))
            has_more = response.get("has_more", False)
            start_cursor = response.get("next_cursor")

        # 중첩된 블록 처리
        processed_blocks = []
        for block in blocks:
            if block.get("has_children", False):
                # 자식 블록 가져오기
                child_response = self.notion.blocks.children.list(block_id=block["id"])
                child_blocks = child_response.get("results", [])
                # 부모 블록의 내용과 자식 블록들을 함께 처리
                block["children"] = child_blocks
            processed_blocks.append(block)

        print(f"blocks: \n{processed_blocks}")
        content = self.notion_blocks_to_markdown(processed_blocks)
        print(f"content: \n{content}")
        return title, content

    def apply_text_formatting(self, item):
        """
        Apply text formatting (annotations and href links) to a Notion text item
        """
        # Get plain text content
        text = item.get("plain_text", "")

        # Skip processing if text is empty or just whitespace
        if not text.strip():
            return text

        # Get annotations
        annotations = item.get("annotations", {})

        # Fix spaces in text if needed for proper formatting
        # Trim leading space if it will interfere with bold/italic formatting
        if annotations.get("bold") or annotations.get("italic"):
            text = text.strip()

        # Apply text formatting based on annotations
        if annotations.get("bold"):
            text = f"**{text}**"
        if annotations.get("italic"):
            text = f"*{text}*"
        if annotations.get("strikethrough"):
            text = f"~~{text}~~"
        if annotations.get("underline"):
            text = f"__{text}__"
        if annotations.get("code"):
            text = f"`{text}`"

        # Add link if href exists
        href = item.get("href")
        if href:
            text = f"[{text}]({href})"

        return text

    def notion_2_md_title(self, json_notion):
        json_property = json_notion.get("properties")

        # Title extraction from 'title' property
        title_items = json_property.get("title", {}).get("title", [])
        result = ""

        for item in title_items:
            result += self.apply_text_formatting(item)

        return result

    def notion_blocks_to_markdown(self, blocks):
        markdown_content = ""
        for block in blocks:
            # Process the current block
            markdown_content += self.notion_block_to_markdown(block)
        return markdown_content

    def notion_block_to_markdown(self, block):
        block_type = block.get("type")
        if block_type == "paragraph":
            return self.notion_paragraph_to_markdown(block)
        elif block_type == "heading_1":
            return self.notion_heading_1_to_markdown(block)
        elif block_type == "heading_2":
            return self.notion_heading_2_to_markdown(block)
        elif block_type == "heading_3":
            return self.notion_heading_3_to_markdown(block)
        elif block_type == "bulleted_list_item":
            return self.notion_bulleted_list_item_to_markdown(block)
        elif block_type == "numbered_list_item":
            return self.notion_numbered_list_item_to_markdown(block)
        elif block_type == "to_do":
            return self.notion_to_do_to_markdown(block)
        elif block_type == "toggle":
            return self.notion_toggle_to_markdown(block)
        elif block_type == "quote":
            return self.notion_quote_to_markdown(block)
        elif block_type == "divider":
            return self.notion_divider_to_markdown(block)
        elif block_type == "callout":
            return self.notion_callout_to_markdown(block)
        elif block_type == "code":
            return self.notion_code_to_markdown(block)
        elif block_type == "image":
            return self.notion_image_to_markdown(block)
        elif block_type == "bookmark":
            return self.notion_bookmark_to_markdown(block)
        elif block_type == "link_preview":
            return self.notion_link_preview_to_markdown(block)
        elif block_type == "table":
            return self.notion_table_to_markdown(block)
        else:
            return self.notion_default_to_markdown(block)

    def notion_heading_1_to_markdown(self, block):
        rich_text_items = block.get("heading_1", {}).get("rich_text", [])
        markdown_text = "# "
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)
        # markdown_text = self.ensure_line_breaks(markdown_text)
        return markdown_text + "  \n\n"

    def notion_heading_2_to_markdown(self, block):
        rich_text_items = block.get("heading_2", {}).get("rich_text", [])
        markdown_text = "## "
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)
        # markdown_text = self.ensure_line_breaks(markdown_text)
        return markdown_text + "  \n\n"

    def notion_heading_3_to_markdown(self, block):
        rich_text_items = block.get("heading_3", {}).get("rich_text", [])
        markdown_text = "### "
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)
        # markdown_text = self.ensure_line_breaks(markdown_text)
        return markdown_text + "  \n\n"

    # def ensure_line_breaks(self, text):
    # """
    # Add two spaces after periods, question marks, and exclamation marks
    # to ensure proper line breaks in Markdown.
    # """
    # if not text:
    #     return text

    # processed = text.replace(". ", ".  ")
    # processed = processed.replace("? ", "?  ")
    # processed = processed.replace("! ", "!  ")
    # return processed

    def notion_paragraph_to_markdown(self, block):
        rich_text_items = block.get("paragraph", {}).get("rich_text", [])
        markdown_text = ""
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)

        # Ensure proper line breaks
        # markdown_text = self.ensure_line_breaks(markdown_text)

        return markdown_text + "  \n" if markdown_text else "\n"

    def notion_bulleted_list_item_to_markdown(self, block):
        rich_text_items = block.get("bulleted_list_item", {}).get("rich_text", [])
        markdown_text = "* "

        # Process the text content
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)

        # Ensure proper line breaks
        # markdown_text = self.ensure_line_breaks(markdown_text)

        # Handle nested children if any
        if block.get("has_children") and "children" in block:
            markdown_text += "  \n"
            indented_content = ""

            for child_block in block.get("children", []):
                child_content = self.notion_block_to_markdown(child_block)
                # Indent nested content
                for line in child_content.split("\n"):
                    if line.strip():
                        # line = self.ensure_line_breaks(line)
                        indented_content += "  " + line + "  \n"  # 2-space indent for nested content

            markdown_text += indented_content

        return markdown_text + "  \n"

    def notion_numbered_list_item_to_markdown(self, block):
        rich_text_items = block.get("numbered_list_item", {}).get("rich_text", [])
        markdown_text = "1. "  # The actual numbering will be handled by Markdown

        # Process the text content
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)

        # Ensure proper line breaks
        # markdown_text = self.ensure_line_breaks(markdown_text)

        # Handle nested children if any
        if block.get("has_children") and "children" in block:
            markdown_text += "  \n"
            indented_content = ""

            for child_block in block.get("children", []):
                child_content = self.notion_block_to_markdown(child_block)
                # Indent nested content
                for line in child_content.split("\n"):
                    if line.strip():
                        # line = self.ensure_line_breaks(line)
                        indented_content += "   " + line + "  \n"  # 3-space indent for nested content

            markdown_text += indented_content

        return markdown_text + "  \n"

    def notion_to_do_to_markdown(self, block):
        todo_data = block.get("to_do", {})
        rich_text_items = todo_data.get("rich_text", [])
        checked = todo_data.get("checked", False)

        markdown_text = "- [" + ("x" if checked else " ") + "] "
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)

        # markdown_text = self.ensure_line_breaks(markdown_text)
        return markdown_text + "  \n"

    def notion_toggle_to_markdown(self, block):
        toggle_data = block.get("toggle", {})
        rich_text_items = toggle_data.get("rich_text", [])

        markdown_text = "<details>\n<summary>"
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)
        markdown_text += "</summary>\n\n"

        # If the toggle has children, add their content
        if block.get("has_children") and "children" in block:
            for child_block in block.get("children", []):
                markdown_text += self.notion_block_to_markdown(child_block)

        markdown_text += "</details>\n\n"
        return markdown_text

    def notion_quote_to_markdown(self, block):
        rich_text_items = block.get("quote", {}).get("rich_text", [])
        markdown_text = "> "

        # Process the quote text
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)
        markdown_text += "  \n"

        # If the quote has children, add them with proper indentation
        if block.get("has_children") and "children" in block:
            for child_block in block.get("children", []):
                # Process each child block and add it with proper indentation
                child_content = self.notion_block_to_markdown(child_block)

                # Add '> ' prefix to each line of the child content to maintain the quote formatting
                indented_content = ""
                for line in child_content.split("\n"):
                    if line.strip():
                        indented_content += "> " + line + "  \n"
                    else:
                        indented_content += ""

                markdown_text += indented_content

        # Add an extra line break after the quote
        markdown_text += "\n"
        return markdown_text

    def notion_divider_to_markdown(self, block):
        return "---\n\n"

    def notion_callout_to_markdown(self, block):
        callout_data = block.get("callout", {})
        rich_text_items = callout_data.get("rich_text", [])
        icon = callout_data.get("icon", {})

        # Handle emoji icon if present
        icon_prefix = ""
        if icon.get("type") == "emoji":
            icon_prefix = icon.get("emoji", "") + " "

        markdown_text = "> **" + icon_prefix + "**"
        for item in rich_text_items:
            markdown_text += self.apply_text_formatting(item)

        # markdown_text = self.ensure_line_breaks(markdown_text)
        return markdown_text + "  \n\n"

    def notion_code_to_markdown(self, block):
        code_data = block.get("code", {})
        rich_text_items = code_data.get("rich_text", [])
        language = code_data.get("language", "")

        # Get the code content
        code_content = ""
        for item in rich_text_items:
            code_content += item.get("plain_text", "")

        return f"```{language}\n{code_content}\n```\n\n"

    def notion_image_to_markdown(self, block):
        image_data = block.get("image", {})
        caption_items = image_data.get("caption", [])

        # Get the image URL based on type
        image_url = ""
        if image_data.get("type") == "file":
            image_url = image_data.get("file", {}).get("url", "")
        elif image_data.get("type") == "external":
            image_url = image_data.get("external", {}).get("url", "")

        # Get the caption if any
        caption = ""
        for item in caption_items:
            caption += item.get("plain_text", "")

        return f"![{caption}]({image_url})\n\n"

    def notion_bookmark_to_markdown(self, block):
        bookmark_data = block.get("bookmark", {})
        url = bookmark_data.get("url", "")
        caption_items = bookmark_data.get("caption", [])

        # Get the caption if any
        caption = ""
        for item in caption_items:
            caption += self.apply_text_formatting(item)

        markdown = f"[{url}]({url})"
        if caption:
            markdown += f" - {caption}"
        return markdown + "\n\n"

    def notion_link_preview_to_markdown(self, block):
        link_preview_data = block.get("link_preview", {})
        url = link_preview_data.get("url", "")
        return f"[Preview]({url})\n\n"

    def notion_table_to_markdown(self, block):
        table_data = block.get("table", {})
        has_column_header = table_data.get("has_column_header", False)

        # If the table has children (rows), process them
        markdown_text = ""
        if block.get("has_children") and "children" in block:
            rows = block.get("children", [])
            if not rows:
                return markdown_text

            # Get the number of columns from the first row
            first_row = rows[0] if rows else {}
            first_row_data = first_row.get("table_row", {})
            cells = first_row_data.get("cells", [])
            num_columns = len(cells)

            # Create table header
            for i in range(num_columns):
                markdown_text += "| Column " + str(i + 1) + " "
            markdown_text += "|\n"

            # Create separator row
            for _ in range(num_columns):
                markdown_text += "| --- "
            markdown_text += "|\n"

            # Process each row
            for i, row in enumerate(rows):
                # Skip the first row if it's used as a header
                if i == 0 and has_column_header:
                    continue

                row_data = row.get("table_row", {})
                cells = row_data.get("cells", [])

                for cell in cells:
                    cell_text = ""
                    for item in cell:
                        cell_text += self.apply_text_formatting(item)
                    markdown_text += "| " + cell_text + " "
                markdown_text += "|\n"

            markdown_text += "\n"

        return markdown_text

    def notion_default_to_markdown(self, block):
        # For unsupported block types, return an empty string
        block_type = block.get("type", "unknown")
        return f"<!-- Unsupported block type: {block_type} -->\n\n"
