from PyPDF2 import PdfReader
from io import BytesIO
import requests
import asyncio
from prisma import Prisma
import pytesseract
from PIL import Image
import fitz
import pymupdf
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR\tesseract.exe"


# client = genai.Client(api_key="AIzaSyC_eBiiJRNneilrj30VnwsK2JkhoIG6Jlk")

def pdfFromLink(pdf_url):
    response = requests.get(pdf_url)
    response.raise_for_status()  # Raise an exception for bad responses (4xx or 5xx)
    pdf_file = BytesIO(response.content)
    return pdf_file

def pdfToList (pdf, start=0, end=-1):
  ans = []
  reader = PdfReader(pdf)
  size = len(reader.pages)
  
  if (end==-1):
      end = size
  
  for i in range(start, end):
    page = reader.pages[i]
    ans.append(page.extract_text())
  return ans

import google.generativeai as genai

# Configure the API key once globally
genai.configure(api_key="AIzaSyAgHQQjyVKDs_AOydJCfO25oqNpKUcsigw")

# Load the model once (outside the function to avoid repeated loading)
model = genai.GenerativeModel("gemini-1.5-flash")  # or "gemini-pro"

def getGeminiResponse(query: str) -> str:
    try:
        response = model.generate_content(query)
        return response.text.replace("```json", "").replace("```", "").strip()
    except Exception as e:
        print("Error during Gemini response:", e)
        return "{}"


async def pushTablesInSequence(pdf, start, end, pdfLink):
    
    db = Prisma()
    
    print("session started")
    
    await db.connect()

    reader = PdfReader(pdf)
    
    size = len(reader.pages)
    
    if(end==-1):
        end = size

    for i in range(start, end):
        page = reader.pages[i]

        pageText = page.extract_text()

        if(pageText == ""):
            pdf_document_images = fitz.open(stream= pdf.getvalue(), filetype="pdf")
            page_image = pdf_document_images[i]
            zoom = 600 / 72
            matrix = fitz.Matrix(zoom, zoom)
            pix = page_image.get_pixmap(matrix=matrix)
            # Convert to PIL Image
            pil_img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
 
            pageText = pytesseract.image_to_string(pil_img)
        
        tableData =  getGeminiResponse(f"""
                                       the following text is extracted from a pdf. it may contain paragraphs and tables.
                                       you have to read that text and convert it to a usable format.
                                       I want you to convert it to a JSON

                                       please ensure that your output is a valid JSON. and represents the original table as close as possible.
                                       response only in JSON format, and no extra text/words shall be in your response.

                                       the output may be a nested json, and its keys should map to the heder of the table. please do not separate the header and data.
                                       please make sure you do not hallucinate write any number data with precision and accuracy as it represents sensitive statistical data.
                                       
                                       Raw Text is as follows:
                                       {pageText}
                                        """)
        title = getGeminiResponse(f"Please output a suitable title for the provided raw text. do not write anything other than the title in your response. raw Text: {pageText}")
        
        description = getGeminiResponse(f"Please output a suitable brief description for the provided raw text. do not write anything other than the description in your response. raw Text: {pageText}")

        keywords = getGeminiResponse(f"Please output a list of 5 keys corresponding to the provided raw text. do not write anything other than the keys list in your response. raw Text: {pageText}")

        data_to_push = {
            "title": title,
            "description": description,
            "keywords": keywords,
            "table_json": tableData, 
            "docOfOrigin": pdfLink
        }
    
        pushed = await db.tablerecord.create(data = data_to_push)
    
        print(f'\n\nPushed data:\n{pushed}\n\n')

        
    await db.disconnect()
    return "Done Successfully"


links = [
    "https://www.mospi.gov.in/sites/default/files/publication_reports/AnnualReport_PLFS2023-24L2.pdf",
    "https://www.mospi.gov.in/sites/default/files/publication_reports/nss_rep_377.pdf"
]

# async def pushToDB() -> None:
#     db = Prisma()


# async def main() -> None:
#     db = Prisma()
#     print("session started")
#     await db.connect()
#     myPdf = pdfFromLink(links[1])
    
#     to_push = pushTablesInSequence(myPdf, 29,30, links[1])
    
#     pushed = await db.tablerecord.create(data = to_push)
    

#     print(f'\n\nPushed data:\n{pushed}\n\n')
    
#     await db.disconnect()

# if __name__ == '__main__':
#     asyncio.run(main())

async def localPush(pdfName, start, end, origin):
    db = Prisma()
    
    print("session started")
    
    await db.connect()

    reader = PdfReader(pdfName)
    
    size = len(reader.pages)
    
    if(end==-1):
        end = size

    for i in range(start, end):
        page = reader.pages[i]

        pageText = page.extract_text()

        if(pageText == ""):
            pdf = pymupdf.open(pdfName)
            pdf_document_images = fitz.open(stream= pdf.getvalue(), filetype="pdf")
            page_image = pdf_document_images[i]
            zoom = 600 / 72
            matrix = fitz.Matrix(zoom, zoom)
            pix = page_image.get_pixmap(matrix=matrix)
            # Convert to PIL Image
            pil_img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
 
            pageText = pytesseract.image_to_string(pil_img)
        
        tableData =  getGeminiResponse(f"""
                                       the following text is extracted from a pdf. it may contain paragraphs and tables.
                                       you have to read that text and convert it to a usable format.
                                       I want you to convert it to a JSON

                                       please ensure that your output is a valid JSON. and represents the original table as close as possible.
                                       response only in JSON format, and no extra text/words shall be in your response.

                                       the output may be a nested json, and its keys should map to the heder of the table. please do not separate the header and data.
                                       please make sure you do not hallucinate write any number data with precision and accuracy as it represents sensitive statistical data.
                                       
                                       Raw Text is as follows:
                                       {pageText}
                                        """)
        title = getGeminiResponse(f"Please output a suitable title for the provided raw text. do not write anything other than the title in your response. raw Text: {pageText}")
        
        description = getGeminiResponse(f"Please output a suitable brief description for the provided raw text. do not write anything other than the description in your response. raw Text: {pageText}")

        keywords = getGeminiResponse(f"Please output a list of 5 keys corresponding to the provided raw text. do not write anything other than the keys list in your response. raw Text: {pageText}")

        data_to_push = {
            "title": title,
            "description": description,
            "keywords": keywords,
            "table_json": tableData, 
            "docOfOrigin": origin
        }
    
        pushed = await db.tablerecord.create(data = data_to_push)
    
        print(f'\n\nPushed data:\n{pushed}\n\n')

        
    await db.disconnect()
    return "Done Successfully"



async def PhotoPush(imgName):
    
    db = Prisma()
    
    print("session started")
    
    await db.connect()
       
    page_image = Image.open(imgName)
    
    pageText = pytesseract.image_to_string(page_image, config="--psm 6")
     
    tableData =  getGeminiResponse(f"""
                                   the following text is extracted from a pdf. it may contain paragraphs and tables.
                                   you have to read that text and convert it to a usable format.
                                   I want you to convert it to a JSON

                                   please ensure that your output is a valid JSON. and represents the original table as close as possible.
                                   response only in JSON format, and no extra text/words shall be in your response.

                                   the output may be a nested json, and its keys should map to the heder of the table. please do not separate the header and data.
                                   please make sure you do not hallucinate write any number data with precision and accuracy as it represents sensitive statistical data.
                                   
                                   Raw Text is as follows:
                                   {pageText}
                                    """)
    title = getGeminiResponse(f"Please output a suitable title for the provided raw text. do not write anything other than the title in your response. raw Text: {pageText}")
     
    description = getGeminiResponse(f"Please output a suitable brief description for the provided raw text. do not write anything other than the description in your response. raw Text: {pageText}")

    keywords = getGeminiResponse(f"Please output a list of 5 keys corresponding to the provided raw text. do not write anything other than the keys list in your response. raw Text: {pageText}")

    data_to_push = {
        "title": title,
        "description": description,
        "keywords": keywords,
        "table_json": tableData, 
        "docOfOrigin": 'image'
    }
    
    pushed = await db.tablerecord.create(data = data_to_push)
    
    print(f'\n\nPushed data:\n{pushed}\n\n')

        
    await db.disconnect()
    return "Done Successfully"

async def main():
    await PhotoPush("photo4.png")

if __name__ == "__main__":
    asyncio.run(main())
