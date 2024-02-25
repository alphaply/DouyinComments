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
    parser.add_argument('--cookis', type=str, help='抖音网站的cookies',
                        default="ttwid=1%7CZhDR_EF4eGGiIxGKXCkG_YpYMWeBnAdF7ITKpi1dI1g%7C1706790094%7C59bd4293120b38700a2b6fc9014566a70a537b48f30817a1fa3807dc235f2c4f; passport_csrf_token=8ff4c90eec1f0ae0b131747f185f93e2; passport_csrf_token_default=8ff4c90eec1f0ae0b131747f185f93e2; bd_ticket_guard_client_web_domain=2; __ac_nonce=065d5cacd004b6e18d8b8; __ac_signature=_02B4Z6wo00f01gDNMeQAAIDDCuvqTMAixQ4A7TVAAOX0yd3WIEmJ5-JTsYKqMByG9YVrCoyrqoAE2bcFN50Dcn3t6LXY2iR-RspqYRZ4bhYeUVnYs57UFVr3b0JgNTmQHKN5gUsYunYuPHyy06; douyin.com; device_web_cpu_core=32; device_web_memory_size=8; architecture=amd64; dy_swidth=2560; dy_sheight=1440; stream_recommend_feed_params=%22%7B%5C%22cookie_enabled%5C%22%3Atrue%2C%5C%22screen_width%5C%22%3A2560%2C%5C%22screen_height%5C%22%3A1440%2C%5C%22browser_online%5C%22%3Atrue%2C%5C%22cpu_core_num%5C%22%3A32%2C%5C%22device_memory%5C%22%3A8%2C%5C%22downlink%5C%22%3A10%2C%5C%22effective_type%5C%22%3A%5C%224g%5C%22%2C%5C%22round_trip_time%5C%22%3A50%7D%22; FORCE_LOGIN=%7B%22videoConsumedRemainSeconds%22%3A180%7D; csrf_session_id=95672e1bd2834bef10896ffa21fc11db; strategyABtestKey=%221708509902.992%22; volume_info=%7B%22isUserMute%22%3Afalse%2C%22isMute%22%3Atrue%2C%22volume%22%3A0.5%7D; xgplayer_user_id=872412523777; s_v_web_id=verify_lsvmoz2h_f28e4a75_1235_56df_3b45_d7590be02127; passport_assist_user=CkGBnAMv99dWCr_OVK7804te5hZzG_rKIL30PhY1IdshfG2GGZXvxKVMcaK9vFvMKTa1PVuTw75wCQCrbR1QjdyLWBpKCjyGctGfv5y4OlShyTWuNIXPqHzIS2j99v0DWkMAtvJpDPNEFl05nM_MzWjCaY1kFdwgSDQBpj0umVSYytYQrvzJDRiJr9ZUIAEiAQPAwkGH; n_mh=21v8GD30t3e1hdh_cEu4pqgmGGC4saJHua7I1gOM2l8; sso_auth_status=be43f5771dc1f8638dbaf38a04a351b2; sso_auth_status_ss=be43f5771dc1f8638dbaf38a04a351b2; sso_uid_tt=ee6c9c80b8a460c1b1716381f95a5f0f; sso_uid_tt_ss=ee6c9c80b8a460c1b1716381f95a5f0f; toutiao_sso_user=a5d94992e6ea56c01143bc75e0d810aa; toutiao_sso_user_ss=a5d94992e6ea56c01143bc75e0d810aa; sid_ucp_sso_v1=1.0.0-KGViMGY2ZWJiMWY2MzZjYjA0ZjdlYmM5MTQzY2M5YzlkOWUyMmYzZGYKHwiH6ZDoqvSLBBDyldeuBhjvMSAMMOuhq_kFOAJA8QcaAmxxIiBhNWQ5NDk5MmU2ZWE1NmMwMTE0M2JjNzVlMGQ4MTBhYQ; ssid_ucp_sso_v1=1.0.0-KGViMGY2ZWJiMWY2MzZjYjA0ZjdlYmM5MTQzY2M5YzlkOWUyMmYzZGYKHwiH6ZDoqvSLBBDyldeuBhjvMSAMMOuhq_kFOAJA8QcaAmxxIiBhNWQ5NDk5MmU2ZWE1NmMwMTE0M2JjNzVlMGQ4MTBhYQ; passport_auth_status=8e95ec652187816f8ff7bc3798d50da2%2C72455723182191af0d165b8798ef0eba; passport_auth_status_ss=8e95ec652187816f8ff7bc3798d50da2%2C72455723182191af0d165b8798ef0eba; uid_tt=479aa5ebaee038f5687eee178214e31b; uid_tt_ss=479aa5ebaee038f5687eee178214e31b; sid_tt=3e75b870fb568a22fd1716f5b30e8612; sessionid=3e75b870fb568a22fd1716f5b30e8612; sessionid_ss=3e75b870fb568a22fd1716f5b30e8612; xg_device_score=7.811101711487342; publish_badge_show_info=%220%2C0%2C0%2C1708509941305%22; LOGIN_STATUS=1; FOLLOW_LIVE_POINT_INFO=%22MS4wLjABAAAA3Zs1lkiHdHYRdfEe9OUnfgsl1TNY3UOj6WDHnpTSOy9gp8XrQXSMY-7VjQsiBTfP%2F1708531200000%2F0%2F1708509941644%2F0%22; store-region=cn-zj; store-region-src=uid; _bd_ticket_crypt_doamin=2; _bd_ticket_crypt_cookie=9970403e0a9d32767b73130d72942f08; __security_server_data_status=1; d_ticket=8722df1444ef36a9d19cb752209a876f931a8; sid_guard=3e75b870fb568a22fd1716f5b30e8612%7C1708509959%7C5183983%7CSun%2C+21-Apr-2024+10%3A05%3A42+GMT; sid_ucp_v1=1.0.0-KDhiZmI5NzliOGMxMmUxOWIxNzNkMThkOWI0NDRhNzAwMTZjZWY2OTQKGwiH6ZDoqvSLBBCHlteuBhjvMSAMOAJA8QdIBBoCaGwiIDNlNzViODcwZmI1NjhhMjJmZDE3MTZmNWIzMGU4NjEy; ssid_ucp_v1=1.0.0-KDhiZmI5NzliOGMxMmUxOWIxNzNkMThkOWI0NDRhNzAwMTZjZWY2OTQKGwiH6ZDoqvSLBBCHlteuBhjvMSAMOAJA8QdIBBoCaGwiIDNlNzViODcwZmI1NjhhMjJmZDE3MTZmNWIzMGU4NjEy; passport_fe_beating_status=true; bd_ticket_guard_client_data=eyJiZC10aWNrZXQtZ3VhcmQtdmVyc2lvbiI6MiwiYmQtdGlja2V0LWd1YXJkLWl0ZXJhdGlvbi12ZXJzaW9uIjoxLCJiZC10aWNrZXQtZ3VhcmQtcmVlLXB1YmxpYy1rZXkiOiJCRk1YR0l6VCt6Y3RJS1FzU29IcGtjcitWR1Riam51SUh4QmV1SmJKTWw5MTdBWGtNL1NTay94dXRia3AvellXK3hZekMyV3JFY0ZLQ2F1TE8zR2V5U1k9IiwiYmQtdGlja2V0LWd1YXJkLXdlYi12ZXJzaW9uIjoxfQ%3D%3D; msToken=LDRTYzOKGzu68xUX303HFnDMJJS298PzD__a7lsRGn3b0HZQCDvYMR1bBH3_jgSz5n-6fMJZwjdsB0peqF02SozlwE8MCl1Tt-yxQ6B6aqH6PfE4P_UwKxNChw==; home_can_add_dy_2_desktop=%221%22; msToken=dvp5l4yPxkWLwpALiGbIGcyU-XIp4h4pcthPksMarJ7Wo3Cbios3BFKfAdXixfuJn0hSK3sMyPE9lau4VSeJp8gMyNxD_AloghLBkruRr7DrWj0MfL-WDM-vZg==; odin_tt=f26e352f5aae4da1a489b5448d5ee3ee661c936326aea9c12a1f381df52bd97f15b1430db8e84f03af993bbfe4118c68938612886eca663d3bfc043c4df3e07f; tt_scid=y6FjtxMwKLsDwqxJOAaoVGYwVDg2CFQJ1iBwDYwe1VKfwv4sv6Ei1jBhunvkfA-9c523; pwa2=%220%7C0%7C1%7C0%22; stream_player_status_params=%22%7B%5C%22is_auto_play%5C%22%3A0%2C%5C%22is_full_screen%5C%22%3A0%2C%5C%22is_full_webscreen%5C%22%3A0%2C%5C%22is_mute%5C%22%3A1%2C%5C%22is_speed%5C%22%3A1%2C%5C%22is_visible%5C%22%3A1%7D%22; IsDouyinActive=true")
    return parser


async def main():
    global headers
    # 获取所有评论
    parser = create_parser()
    args = parser.parse_args()
    aweme_id = args.aweme_id
    cookies = args.cookis
    headers['Cookie'] = cookies

    all_comments = await fetch_all_comments_async(aweme_id)
    print(f"Found {len(all_comments)} comments.")
    all_replys = await fetch_all_replys_async(all_comments)
    print(f"Found {len(all_replys)} replies in total.")
    print(len(all_replys) + len(all_comments))

    all_comments = process_comments(all_comments)
    save(all_comments, "comments.csv")
    all_replys = process_replys(all_replys, all_comments)
    save(all_replys, "replys.csv")


# 运行 main 函数
if __name__ == "__main__":
    asyncio.run(main())
    print('done!')
