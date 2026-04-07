import scrapy
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import time
from mongo_db import ThuVienNhaDat
from random import uniform
from tqdm import tqdm

def get_metadata():
    """
    Hàm trả về metadata thu thập dữ liệu
    """
    now = datetime.now()
    metadata = {
        "Date": now.strftime("%Y-%m-%d"),       # ví dụ: '2025-06-11'
        "Time": now.strftime("%H:%M:%S")        # ví dụ: '17:36:27'
    }
    return metadata

def scraping_details (link: str):
    '''
    Lấy thông tin chi tiết từng bài đăng:
        + Tiêu đề : Post Title
        + Giá tiền/m2 : price_per_m2
        + Thông tin bài đăng: Post Detail
        + Thuộc tính căn nhà: House Information
    '''
    try:
        response_pages = requests.get(link, timeout=10)
        response_pages.raise_for_status()
    except requests.RequestException as e:
        print(f"Lỗi khi tải trang: {e}")
    # Page HTML
    soup_pages = BeautifulSoup (response_pages.text, "html.parser")
    
    # Post Title 
    post_title = soup_pages.select_one ('h1')
    post_title = post_title.get_text(strip=True) if post_title else None
    
    # Price per m2
    price_per_m2 = soup_pages.select_one (
        'div[class="ui horizontal borderless segments"]>div[class="ui segment p-0"]>div>span'
    )
    price_per_m2 = price_per_m2.get_text(strip=True) if price_per_m2 else None
    
    # Post detail
    detail_tags = soup_pages.select ('aside[class="ui segment"] > p')
    detail = "\n".join(d.get_text(strip=True) for d in detail_tags) if detail_tags else None
    
    # House Information
    attributes = soup_pages.select ('div[id="grid-realestate-feature"] > div[class="row"] > div[class="column info-estate"]')
    house_info = {}
    for attr in attributes:
        key = attr.select_one ('div.eight.wide.column.unit-name-style').get_text ().strip ()
        value = attr.select_one ('div.eight.wide.column.text-muted.floated.end').get_text ().strip ()
        
        house_info[key] = value
        
    return post_title, price_per_m2, detail, house_info

def scraping_pages (
    start_page: int = 1, 
    num_pages: int = 1, 
    delay: tuple = (1, 3)
):
    total_posts = 0 # Count the number of posts
    base_url = "https://thuviennhadat.vn"
    for page_index in tqdm(range(start_page, num_pages + 1), desc="Trang", unit="trang"):
        url = f"{base_url}/ban-nha-dat-toan-quoc?trang={page_index}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
        except requests.RequestException:
            tqdm.write(f"Lỗi tải trang {page_index}")
            continue
        
        # Page HTML
        soup = BeautifulSoup (response.text, "html.parser")
        
        if page_index < 5:
            articles = soup.select ("div[class='navigateTo mb-mt-15'] > div[class='ui divided items mobile-not-show']")
        else:
            articles = soup.select('aside[class="ui segment mobile-container-list"] > a')
        for article in articles:
            # Get URL
            if page_index < 5:
                link = article.select_one ("a").get ("href")
            else:
                link = article.get('href')
                
            if link and link.startswith("/"):
                link = base_url + link

            # Get Post Date
            post_date = article.select_one (
                "div.KinhDoanhNhaDat-index-post-footer span"
            )
            post_date = post_date.get_text(strip=True) if post_date else None
            
            # Get Location
            location = article.select_one ("div.ui.labeled.icon span")
            location = location.get_text(strip=True) if location else None
            
        # Bỏ qua 1 bài đăng duy nhất không xử lý được    
            if (link == None):
                print (f"Link rỗng tại trang {page_index}, post thứ {total_posts}")
                continue
            
            post_title, price_per_m2, detail, house_info = scraping_details (link)
            
                # Count Posts
            if post_title:
                total_posts += 1
                
            # Get Metadata
            metadata = get_metadata ()
            data = {
                "metadata": metadata,
                "link": link,
                "post title": post_title,
                "post date": post_date,
                "location": location,
                "price_per_m2": price_per_m2,
                "text": detail,
                "details" : house_info,
                "page" : page_index
            }
            try:
                ThuVienNhaDat.insert_one (data)
            except Exception as e:
                print (f"Lỗi ở bài đăng, trang thứ {total_posts}, link: {link}: {e}.")
            
        # time.sleep(uniform(*delay)) # NHÂN ĐẠO 
            
    tqdm.write(f"\nHoàn tất! Tổng cộng {total_posts} bài đăng.")

