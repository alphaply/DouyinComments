# 抖音评论爬虫脚本

本脚本用于爬取指定抖音视频的评论及其回复，并将结果保存为CSV文件。

参考项目：https://github.com/ShilongLee/Crawler

抖音对这部分接口进行了加密，本项目直接参考该项目中处理params，headers的部分

感谢ShilongLee!

## 环境要求

脚本运行需要以下环境：
- asyncio
- httpx
- pandas
- execjs
- cookiesparser
- nodejs（重要）


## 安装依赖

在运行脚本之前，请确保安装了所有必要的依赖,别忘记安装**nodejs**：

```bash
pip install -r requirements.txt
```

## 脚本运行

请先编辑脚本中的cookie变量，将其替换为您自己的cookie。然后运行脚本

随后根据提示输入awesome_id即可！



## 输出
脚本将在当前目录下生成两个CSV文件：

- comments.csv：包含视频的所有评论信息。
- replies.csv：包含所有评论的回复信息。

每个文件中都会包含评论或回复的详细信息，如评论内容、点赞数、评论时间、用户昵称等。

## 日志
新增获取评论回复图片
