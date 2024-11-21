import asyncio
import httpx
import pandas as pd
from tqdm import tqdm
from datetime import datetime
from typing import Any
import os
from common import common

url = "https://www.douyin.com/aweme/v1/web/comment/list/"

with open('cookie.txt', 'r') as f:
    cookie = f.readline().strip()

async def get_comments_async(client: httpx.AsyncClient, aweme_id: str, cursor: str = "0", count: str = "50") -> dict:
    params = {"aweme_id": aweme_id, "cursor": cursor, "count": count, "item_type": 0}
    headers = {"cookie": cookie}
    params, headers = common(url, params, headers)
    response = await client.get(url, params=params, headers=headers)
    await asyncio.sleep(0.8)
    try:
        return response.json()
    except ValueError:
        return {}

async def fetch_all_comments_async(aweme_id: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(timeout=600) as client:
        cursor = 0
        all_comments = []
        has_more = 1
        with tqdm(desc="Fetching comments", unit="comment") as pbar:
            while has_more:
                response = await get_comments_async(client, aweme_id, cursor=str(cursor))
                comments = response.get("comments", [])
                if isinstance(comments, list):
                    all_comments.extend(comments)
                    pbar.update(len(comments))
                has_more = response.get("has_more", 0)
                if has_more:
                    cursor = response.get("cursor", 0)
                await asyncio.sleep(1)
        return all_comments

def process_comments(comments: list[dict[str, Any]]) -> pd.DataFrame:
    data = [{
        "评论ID": c['cid'],
        "评论内容": c['text'],
        "评论图片": c['image_list'][0]['origin_url']['url_list'] if c['image_list'] else None,
        "点赞数": c['digg_count'],
        "评论时间": datetime.fromtimestamp(c['create_time']).strftime('%Y-%m-%d %H:%M:%S'),
        "用户昵称": c['user']['nickname'],
        "用户主页链接": f"https://www.douyin.com/user/{c['user']['sec_uid']}",
        "用户抖音号": c['user'].get('unique_id', '未知'),
        "用户签名": c['user'].get('signature', '未知'),
        "回复总数": c['reply_comment_total'],
        "ip归属":c['ip_label']
    } for c in comments]
    return pd.DataFrame(data)

def save(data: pd.DataFrame, filename: str):
    data.to_csv(filename, index=False)

async def main():
    aweme_id = input("Enter the aweme_id: ")
    all_comments = await fetch_all_comments_async(aweme_id)
    print(f"Found {len(all_comments)} comments.")
    comments_df = process_comments(all_comments)
    base_dir = f"data/{aweme_id}"
    os.makedirs(base_dir, exist_ok=True)
    comments_file = os.path.join(base_dir, "comments.csv")
    save(comments_df, comments_file)
    print("Comments saved to comments.csv")

if __name__ == "__main__":
    asyncio.run(main())
