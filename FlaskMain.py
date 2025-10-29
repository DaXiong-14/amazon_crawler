# todo 主程序 flask 服务器
import logging

from flask import Flask, request, jsonify
import time
from queue import Queue
from threading import Thread
import uuid

from config.config import PORT
from src.queryData import queryMaster


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s',
        handlers=[
            logging.FileHandler(f'project_flask.log', encoding='utf-8'),
            logging.StreamHandler()
        ]
    )
setup_logging()
logger = logging.getLogger(__name__)

app = Flask(__name__)

# 任务队列
task_queue = Queue()
is_running = False

def task_worker():
    """工作线程，按顺序执行任务"""
    global is_running
    while True:
        # 从队列获取任务（阻塞等待）
        task_data = task_queue.get()
        is_running = True

        try:
            # todo 检验参数
            if 'site' in task_data or 'cid' in task_data or 'p' in task_data:
                raise Exception('缺少重要参数')
            if 'error' in task_data:
                raise Exception(task_data['error'])

            logger.info(f"开始执行任务: {task_data}")
            if task_data.get('p') == 'cn':
                queryMaster(task_data['cid'], task_data['site'], task_data['p'])

            logger.info(f"任务完成: {task_data}")

        except Exception as e:
            logger.error(f"任务执行出错: {e}")
        finally:
            is_running = False
            task_queue.task_done()  # 标记任务完成


# 启动工作线程
worker_thread = Thread(target=task_worker, daemon=True)
worker_thread.start()


@app.route('/api/crawler/task', methods=['POST'])
def submit_task():
    """提交任务接口"""
    try:
        # 获取POST参数
        task_data = request.json

        if not task_data:
            return jsonify({"error": "请提供JSON参数"}), 400

        # 生成任务ID
        task_id = str(uuid.uuid4())[:8]
        task_data['task_id'] = task_id

        # 将任务加入队列
        task_queue.put(task_data)

        return jsonify({
            "status": "success",
            "message": "任务已加入队列",
            "task_id": task_id,
            "queue_size": task_queue.qsize(),
            "current_running": is_running
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/crawler/status', methods=['GET'])
def get_status():
    """查看队列状态"""
    return jsonify({
        "queue_size": task_queue.qsize(),
        "current_running": is_running
    })


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=PORT, debug=True)