# 抖音评论爬虫脚本

本脚本用于爬取指定抖音视频的评论及其回复，并将结果保存为CSV文件。

## 环境要求

脚本运行需要以下环境：
- asyncio
- httpx
- pandas


## 安装依赖

在运行脚本之前，请确保安装了所有必要的依赖：

```bash
pip install httpx pandas
```

## 脚本运行
使用以下命令运行脚本，需要提供视频的ID(aweme_id)和抖音登录后的Cookies(cookies)作为参数：
```bash
python .\main.py --aweme_id 视频的ID --cookies 'xxxxxxx'
```
也可以在`main.py`文件中...
```python
async def main():
    global headers
    # 获取所有评论
    parser = create_parser()
    args = parser.parse_args()
    aweme_id = 具体的id
    cookies = 登录douyin.com之后的cookie
```
## 输出
脚本将在当前目录下生成两个CSV文件：

- comments.csv：包含视频的所有评论信息。
- replys.csv：包含所有评论的回复信息。

每个文件中都会包含评论或回复的详细信息，如评论内容、点赞数、评论时间、用户昵称等。
