import asyncio
import aiohttp
import json
import io
import os
import base64
from openai import AsyncOpenAI, AsyncAzureOpenAI, BadRequestError
from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import ContentSettings
# from ai_models import set_quality
# from logging_utility import log_moderation
from stitch_image_outside import stitch


async def single_character(payload):

    page_num = payload["page_num"]
    preview_id = payload["preview_id"]
    book_id = payload["book_id"]
    user_id = payload["user_id"]
    images = payload["images"]
    desc = payload["description"]
    

    api_key = os.environ["OPENAI_API_KEY"]
    client = AsyncOpenAI(api_key=api_key)

    prompt = await get_prompt(desc[0])
    response = None

    while True:

        try:

            response = await client.images.edit(
                model="gpt-image-1",
                image=images,
                prompt=prompt,
                quality=set_quality
                
            )

            break

        except BadRequestError as e:

            text = str(e).lower()

            if "billing_hard_limit" in text:

                break

            elif "moderation_blocked" in text:

                te = prompt + "\n" + images
                # log_moderation(te)
                new_prompt = """
    📘 Storybook Context:
    This image is intended for use in a children’s storybook. The
    scene should convey gentle, age-appropriate emotion with
    visual storytelling that resonates with young readers. The tone
    must remain warm, sensitive, and suitable for a family-friendly
    narrative.
                """

                prompt = new_prompt + prompt

    if not response:
        exit()

    image = response.data[0].b64_json
    image = base64.b64decode(image)

    asyncio.create_task(upload(image, user_id, preview_id, book_id, page_num))

    image = io.BytesIO(image)
    image.name = "final_image.png"

    return image


async def single_character_azure(payload):

    page_num = payload["page_num"]
    preview_id = payload["preview_id"]
    book_id = payload["book_id"]
    user_id = payload["user_id"]
    images = payload["images"]
    desc = payload["description"]
    deployment = payload["deployment"]
    text_1 = payload["text"]
    gender = payload["gender"]
    # quality_val_ui = payload["quality_val_ui"] #Remove after testing

    api_key = deployment["api_key"]
    endpoint = deployment["endpoint"]
    deployment_name = deployment["deployment-name"]

    prompt = await get_prompt(desc[0])
    response = None

    client = AsyncAzureOpenAI(
        azure_endpoint=endpoint,
        api_key=api_key,
        api_version="2025-04-01-preview",
        azure_deployment=deployment_name,
    )

    try:

        response = await client.images.edit(
            model="gpt-image-1",
            image=images,
            prompt=prompt,
            n=1,
            quality="low",
            # quality = quality_val_ui
        )

    except Exception as e:

        print(e)

        text = str(e).lower()

        if "billing_hard_limit" in text:

            pass

        elif "moderation_blocked" in text:

            # log_moderation(text)
            pass

    if not response:
        return None

    image = response.model_dump()["data"][0]["b64_json"]
    image = base64.b64decode(image)
    image = io.BytesIO(image)
    image.name = "final_image.png"
    image = stitch(image, text_1, gender)
    image = base64.b64encode(image.read()).decode("utf-8")
    asyncio.create_task(upload(image, user_id, preview_id, book_id, page_num))

    return image


async def upload(image, user_id, preview_id, book_id, page_num):

    connection_string = os.environ["connection_string"]

    blob_service = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service.get_container_client("final-books")

    blob_name = f"{user_id}/{preview_id}/{book_id}/{page_num}.png"
    blob_client = container_client.get_blob_client(blob_name)

    blob_client.upload_blob(
        data=image,
        overwrite=True,
        content_settings=ContentSettings(content_type="image/png"),
    )


async def get_prompt(desc: str):

    prompt = f"""
🎨 Scene & Emotion

Create a hyper-realistic digital painting based on the following scene
description.

TEMPLATE DESCRIPTION (Context Only — Do Not Override)
{desc}
This text is only for disambiguation. Do NOT invent, add, or alter visual
elements based on it.
If any detail here conflicts with the TEMPLATE image, FOLLOW THE TEMPLATE image
exactly.

Use the provided TEMPLATE image (first image) to guide the exact layout —
including scene composition, pose, gestures, background elements, clothing,
lighting, and overall emotional tone. The TEMPLATE also defines the exact
facial expressions, gaze, and head tilt to preserve.

IMAGE PRIORITY
When text and images disagree, FOLLOW THE IMAGES.
• Reference portraits define identity: facial structure, skin tone, hairline &
  hair texture/color, durable facial marks, eyewear, bindi/tilak.
• The TEMPLATE defines everything else: expression, head angle/tilt, pose,
  clothing, body silhouette, hands, props, lighting, background, composition.

👤 Character Identity Replacement (Descriptive Input)
For each described character below, apply the identity from its corresponding
REFERENCE portrait to the matching face in the TEMPLATE. Blend seamlessly into
the TEMPLATE’s lighting and painterly texture.

REFERENCE PORTRAIT DESCRIPTIONS (UNORDERED, UNLABELED)
Below is an unordered list of short descriptions; each corresponds to one
attached REFERENCE portrait, but the order here may NOT match upload order.
Use descriptions only to help identify a match; do NOT synthesize features
from text alone — identity must come from the photos.

HAIR FIDELITY (very important)
• Replace face AND all visible hair within the head region (fringe, temples,
  crown, side edges).
• Match the portrait’s coil/curl/wave diameter (relative), density, direction,
  parting, hairline shape, and visible volume silhouette.
• Preserve garment occlusion: do not extend hair to change how clothing
  overlaps. If length conflicts, match hairline/texture/parting precisely while
  keeping the TEMPLATE hair outline at garment boundaries.

AMBIGUITY / SAFETY
* UNDER NO CIRCUMSTANCES YOU CAN ADD A NEW PERSON OR ALTER CLOTHING/BACKGROUND
  TO FORCE A MATCH
* YOU CANNOT CREATE A NEW CHARACTER WHICH IS NOT PRESENT IN TEMPLATE IMAGE WHEN
  A PERSON OF DIFFERENT AGE GROUP IS ADDED

CARDINALITY
• If there are more portraits than faces, ignore extras.
• If there are more faces than portraits, leave unmatched faces unchanged.
• Ignore any leftover descriptions or portraits that could not be paired.

🎨 Final Output
Blend the characters into the scene with seamless realism and emotional
consistency, matching the tone, detail, and storytelling of the original image.
The result should appear as though the characters naturally belong in the
environment.
    """

    return prompt
