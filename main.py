import httpx
import asyncio
from datetime import datetime
import pandas as pd
from tqdm.asyncio import tqdm_asyncio
from tqdm import tqdm
from common import common
from typing import Any

url = "https://www.douyin.com/aweme/v1/web/comment/list/"
reply_url = url + "reply/"


async def get_comments_async(client: httpx.AsyncClient, aweme_id: str, cursor: str = "0", count: str = "50") -> dict[
    str, Any]:
    params = {"aweme_id": aweme_id, "cursor": cursor, "count": count, "item_type": 0}
    headers = {"cookie": cookie}
    params, headers = common(url, params, headers)
    response = await client.get(url, params=params, headers=headers)
    await asyncio.sleep(0.8)
    return response.json()


async def fetch_all_comments_async(aweme_id: str) -> list[dict[str, Any]]:
    async with httpx.AsyncClient() as client:
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


async def get_replies_async(client: httpx.AsyncClient, comment_id: str, cursor: str = "0", count: str = "50") -> dict[
    str, Any]:
    params = {"cursor": cursor, "count": count, "item_type": 0, "item_id": comment_id, "comment_id": comment_id}
    headers = {"cookie": cookie}
    params, headers = common(reply_url, params, headers)
    response = await client.get(reply_url, params=params, headers=headers)
    await asyncio.sleep(1)
    return response.json()


async def fetch_replies_for_comment(client: httpx.AsyncClient, comment: dict[str, Any], pbar: tqdm) -> list[
    dict[str, Any]]:
    comment_id = comment["cid"]
    has_more = 1
    cursor = 0
    all_replies = []
    while has_more and comment["reply_comment_total"] > 0:
        response = await get_replies_async(client, comment_id, cursor=str(cursor))
        replies = response.get("comments", [])
        if isinstance(replies, list):
            all_replies.extend(replies)
        has_more = response.get("has_more", 0)
        if has_more:
            cursor = response.get("cursor", 0)
        await asyncio.sleep(0.5)
    pbar.update(1)
    return all_replies


async def fetch_all_replies_async(comments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    all_replies = []
    async with httpx.AsyncClient() as client:
        with tqdm(total=len(comments), desc="Fetching replies", unit="comment") as pbar:
            tasks = [fetch_replies_for_comment(client, comment, pbar) for comment in comments]
            results = await asyncio.gather(*tasks)
            for result in results:
                all_replies.extend(result)
    return all_replies


def process_comments(comments: list[dict[str, Any]]) -> pd.DataFrame:
    data = [{
        "评论ID": c['cid'],
        "评论内容": c['text'],
        "点赞数": c['digg_count'],
        "评论时间": datetime.fromtimestamp(c['create_time']).strftime('%Y-%m-%d %H:%M:%S'),
        "用户昵称": c['user']['nickname'],
        "用户主页链接": f"https://www.douyin.com/user/{c['user']['sec_uid']}",
        "用户抖音号": c['user']['unique_id'],
        "用户签名": c['user']['signature'],
        "回复总数": c['reply_comment_total'],
    } for c in comments]
    return pd.DataFrame(data)


def process_replies(replies: list[dict[str, Any]], comments: pd.DataFrame) -> pd.DataFrame:
    data = [
        {
            "评论ID": c["cid"],
            "评论内容": c["text"],
            "点赞数": c["digg_count"],
            "评论时间": datetime.fromtimestamp(c["create_time"]).strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "用户昵称": c["user"]["nickname"],
            "用户主页链接": f"https://www.douyin.com/user/{c['user']['sec_uid']}",
            "用户抖音号": c["user"]["unique_id"],
            "用户签名": c["user"]["signature"],
            "回复的评论": c["reply_id"],
            "具体的回复对象": c["reply_to_reply_id"]
            if c["reply_to_reply_id"] != "0"
            else c["reply_id"],
            "回复给谁": comments.loc[comments['评论ID'] == c["reply_id"], '用户昵称'].values[0]
            if c["reply_to_reply_id"] == "0"
            else c["reply_to_username"],
        }
        for c in replies
    ]
    return pd.DataFrame(data)


def save(data: pd.DataFrame, filename: str):
    data.to_csv(filename, index=False)


aweme_id = input("Enter the aweme_id: ")
cookie = 'your cookie'

async def main():
    # 评论部分
    all_comments = await fetch_all_comments_async(aweme_id)
    print(f"Found {len(all_comments)} comments.")
    all_comments_ = process_comments(all_comments)
    save(all_comments_, "comments.csv")

    # 回复部分 如果不需要直接注释掉
    all_replies = await fetch_all_replies_async(all_comments)
    print(f"Found {len(all_replies)} replies")
    print(f"Found {len(all_replies) + len(all_comments)} in totals")
    all_replies = process_replies(all_replies, all_comments_)
    save(all_replies, "replies.csv")


# 运行 main 函数
if __name__ == "__main__":
    asyncio.run(main())
    print('done!')
