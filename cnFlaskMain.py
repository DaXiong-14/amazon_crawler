# todo 提交任务 flask

import logging

from flask import Flask, request, jsonify
import threading
import uuid

from src.amazon_category_integration_crawler import category_integration_master


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler(f'project.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
setup_logging()
logger = logging.getLogger(__name__)


app = Flask(__name__)

# todo 核心：用于跟踪当前任务的全局变量
current_task_id = None

def background_task(config, task_id):
    """在后台线程中执行的实际任务函数"""
    global current_task_id
    try:
        # todo 在这里执行核心程序
        category_integration_master(config.get('cid'), site=config.get('site'))
    except Exception as e:
        print(f"任务执行出错: {e}")
    finally:
        # 任务完成，无论成功失败，都清空标记
        if current_task_id == task_id:
            current_task_id = None


@app.route('/api/crawler/cn', methods=['POST'])
def start_task():
    global current_task_id

    # todo 检验参数
    config = request.json
    if not config:
        return jsonify({"success": False, "error": "缺少配置参数"}), 400
    if 'site' not in config:
        return jsonify({"success": False, "error": "缺少重要参数 site"}), 400
    if 'cid' not in config:
        return jsonify({"success": False, "error": "缺少重要参数 cid"}), 400

    # todo 关键检查：如果有任务ID存在，说明有任务正在运行
    if current_task_id is not None:
        return jsonify({
            "success": False,
            "error": "系统繁忙，已有任务正在执行，请稍后再试"
        }), 429  # 429 Too Many Requests 状态码很贴切

    # todo 没有任务在运行，则创建新任务
    try:
        # 生成唯一任务ID并标记
        new_task_id = str(uuid.uuid4())
        current_task_id = new_task_id

        # 创建并启动后台线程执行实际任务，不阻塞接口响应
        thread = threading.Thread(target=background_task, args=(config, new_task_id))
        thread.daemon = True  # 设置为守护线程，主进程结束则线程结束
        thread.start()

        return jsonify({
            "success": True,
            "message": "任务提交成功，正在后台执行",
            "task_id": new_task_id
        }), 202
    except Exception as e:
        current_task_id = None  # 发生异常，重置状态
        return jsonify({"success": False, "error": f"任务提交失败: {str(e)}"}), 500

# todo 提供一个手动重置状态的接口，用于处理极端情况
@app.route('/api/crawler/cn/reset', methods=['POST'])
def reset_task():
    global current_task_id
    current_task_id = None
    return jsonify({"success": True, "message": "系统状态已重置，可以接收新任务"})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080, debug=True)