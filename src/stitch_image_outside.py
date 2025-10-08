from PIL import Image, ImageDraw, ImageFont
import io
import html.parser


class HTMLTextParser(html.parser.HTMLParser):

    def __init__(self):

        super().__init__()
        self.text_segments = []
        self.current_styles = []

    def handle_starttag(self, tag, attrs):

        self.current_styles.append(tag)

    def handle_endtag(self, tag):

        if tag in self.current_styles:
            self.current_styles.remove(tag)

    def handle_data(self, data):

        self.text_segments.append((data.strip(), self.current_styles.copy()))


def wrap_text(text, max_width, fonts, draw):

    lines = []
    words = text.split()
    current_line = ""
    current_styles = []

    for word in words:
        test_line = current_line + word + " "
        bbox = draw.textbbox((0, 0), test_line, font=fonts['regular'])
        text_width = bbox[2] - bbox[0]

        if text_width <= max_width:
            current_line = test_line
        else:
            if current_line:
                lines.append((current_line.strip(), current_styles))
            current_line = word + " "
            current_styles = []

    if current_line:
        lines.append((current_line.strip(), current_styles))

    return lines


def stitch(image, text, gender):

    if gender == "boy":
        text = text.replace("She", "He")
        text = text.replace("she", "he")
        text = text.replace("her", "his")
        text = text.replace("Her", "His")
        text = text.replace("girl", "boy")
        text = text.replace("Girl", "Boy")

    elif gender == "girl":
        text = text.replace(" he ", " she ")
        text = text.replace(" He ", " She ")
        text = text.replace("his", "her")
        text = text.replace("His", "Her")
        text = text.replace("boy", "girl")
        text = text.replace("Boy", "Girl")

    text_color = (0, 0, 0)  # Black text
    background_color = (255, 255, 255, 255)  # Opaque white background
    position = (10, 10)  # Position for text in the new top area
    font_size = 30
    padding = 10

    # Open original image
    original_image = Image.open(image)
    if original_image.mode != "RGBA":
        original_image = original_image.convert("RGBA")

    # Load fonts (replace with actual font files)
    fonts = {
        'regular': ImageFont.load_default(size=font_size),
        'bold': ImageFont.load_default(size=font_size),
        'italic': ImageFont.load_default(size=font_size),
        'bold_italic': ImageFont.load_default(size=font_size)
    }

    # Parse HTML
    parser = HTMLTextParser()
    parser.feed(text)
    parser.close()

    # Calculate text dimensions
    max_width = original_image.width - position[0] - padding - 10
    temp_image = Image.new("RGBA", (original_image.width, 1000))
    draw = ImageDraw.Draw(temp_image)
    text_height = 0
    text_width = 0
    lines_with_styles = []
    for segment, styles in parser.text_segments:
        if not segment:
            continue
        font_key = 'regular'
        if 'strong' in styles and 'em' in styles:
            font_key = 'bold_italic'
        elif 'strong' in styles:
            font_key = 'bold'
        elif 'em' in styles:
            font_key = 'italic'
        font = fonts[font_key]
        lines = wrap_text(segment, max_width, fonts, draw)
        for line, _ in lines:
            bbox = draw.textbbox((0, 0), line, font=font)
            line_width = bbox[2] - bbox[0]
            line_height = bbox[3] - bbox[1]
            text_width = max(text_width, line_width)
            text_height += line_height + 5
            lines_with_styles.append((line, font))
    text_height = max(text_height - 5, 0)

    # Create new image with extra space for text
    total_height = text_height + 2 * padding + original_image.height
    new_image = Image.new(
        "RGBA", (original_image.width, total_height), (255, 255, 255, 0)
    )
    draw = ImageDraw.Draw(new_image)

    # Draw white background rectangle for text
    background_rect = (
        position[0] - padding,
        position[1] - padding,
        position[0] + text_width + padding,
        position[1] + text_height + padding
    )
    draw.rectangle(background_rect, fill=background_color)

    # Draw text
    y = position[1]
    for line, font in lines_with_styles:
        draw.text((position[0], y), line, fill=text_color, font=font)
        bbox = draw.textbbox((0, 0), line, font=font)
        line_height = bbox[3] - bbox[1]
        y += line_height + 5

    # Paste original image below text area
    new_image.paste(original_image, (0, text_height + 2 * padding))

    output = io.BytesIO()
    new_image.save(output, format="PNG")
    output.seek(0)

    return output
