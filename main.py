import httpx
import asyncio
from datetime import datetime
import pandas as pd
import argparse

url = "https://www.douyin.com/aweme/v1/web/comment/list/"
reply_url = url + "reply/"
headers = {
    "authority": "www.douyin.com",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
    "Cookie": '',
    "Referer": "https://www.douyin.com/",
    "Sec-Ch-Ua": 'Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121',
    "Sec-Ch-Ua-Mobile": "?0",
    "Sec-Ch-Ua-Platform": "Windows",
    "Sec-Fetch-Dest": "empty",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Site": "same-origin",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
}


async def get_comments_async(client, aweme_id, cursor="0", count="50"):
    params = {
        "device_platform": "webapp",
        "aid": "6383",
        "channel": "channel_pc_web",
        "aweme_id": aweme_id,
        "cursor": cursor,
        "count": count,
    }
    response = await client.get(url, params=params, headers=headers)
    return response.json()


async def fetch_all_comments_async(aweme_id):
    async with httpx.AsyncClient() as client:
        cursor = 0
        all_comments = []
        has_more = 1
        while has_more:
            response = await get_comments_async(client, aweme_id, cursor=str(cursor))
            comments = response.get("comments")
            if isinstance(comments, list):
                all_comments.extend(comments)
            has_more = response.get("has_more")
            if has_more:
                cursor = response.get("cursor")
        return all_comments


async def get_replys_async(client, comment_id, cursor="0", count="50"):
    params = {
        "device_platform": "webapp",
        "aid": "6383",
        "channel": "channel_pc_web",
        "comment_id": comment_id,
        "cursor": cursor,
        "count": count,
    }
    response = await client.get(reply_url, params=params, headers=headers)
    return response.json()


async def fetch_all_replys_async(comments):
    async with httpx.AsyncClient() as client:
        tasks = [fetch_replies_for_comment(client, comment) for comment in comments]
        all_replies = await asyncio.gather(*tasks)
        # 展平回复列表
        return [item for sublist in all_replies for item in sublist]


async def fetch_replies_for_comment(client, comment):
    comment_id = comment["cid"]
    has_more = 1
    cursor = 0
    all_replies = []
    while has_more and comment["reply_comment_total"] > 0:
        response = await get_replys_async(client, comment_id, cursor=str(cursor))
        replies = response.get("comments")
        if isinstance(replies, list):
            all_replies.extend(replies)
        has_more = response.get("has_more")
        if has_more:
            cursor = response.get("cursor")
    return all_replies


def process_comments(comments):
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


def process_replys(replys, comments):
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
        for c in replys
    ]

    return pd.DataFrame(data)


def save(data, filename):
    data.to_csv(filename, index=False)


def create_parser():
    parser = argparse.ArgumentParser(description='抖音评论和回复爬虫')
    parser.add_argument('--aweme_id', type=str, help='抖音视频的ID')
    parser.add_argument('--cookies', type=str, help='抖音网站的cookies')
    return parser


async def main():
    global headers
    # 获取所有评论
    parser = create_parser()
    args = parser.parse_args()
    aweme_id = args.aweme_id
    cookies = args.cookies
    headers['Cookie'] = cookies

    all_comments = await fetch_all_comments_async(aweme_id)
    print(f"Found {len(all_comments)} comments.")
    all_replys = await fetch_all_replys_async(all_comments)
    print(f"Found {len(all_replys)} replies")
    print(f"Found {len(all_replys) + len(all_comments)} in totals")

    all_comments = process_comments(all_comments)
    save(all_comments, "comments.csv")
    all_replys = process_replys(all_replys, all_comments)
    save(all_replys, "replys.csv")


# 运行 main 函数
if __name__ == "__main__":
    asyncio.run(main())
    print('done!')
