# tunatalk.py
from nicegui import ui
import asyncio
import os
from agentflow.core.agent import Agent
from agentflow.core.parcel import BinaryParcel
# from agents.topics import AgentTopics

import yaml
import logging
from app_logger import init_logging

logger: logging.Logger = init_logging()

os.makedirs('temp', exist_ok=True)


class SttRelayAgent(Agent):
    def __init__(self):
        config_path = os.path.join(os.getcwd(), 'config', 'system.yaml')
        with open(config_path, 'r', encoding='utf-8') as f:
            agent_config = yaml.safe_load(f) or {}
        super().__init__(name='stt_relay', agent_config=agent_config)

    async def request_stt(self, file_path: str) -> str:
        with open(file_path, 'rb') as f:
            content = f.read()

        pcl = BinaryParcel({'content': content})

        try:
            loop = asyncio.get_running_loop()
            result_pcl = await loop.run_in_executor(
                None,
                lambda: self.publish_sync("STT/Content", pcl, timeout=600)  # 設定 10 分鐘逾時
            )
            # result_pcl = await loop.run_in_executor(None, lambda: self.publish_sync(AgentTopics.STT_CONTENT, pcl))
            if result_pcl.content:
                return result_pcl.content.get('text', '[錯誤] 無法取得語音辨識結果')
            else:
                return '[錯誤] 無法取得語音辨識結果'
        except Exception as ex:
            logger.exception(f"STT 錯誤：{ex}")
            return "[錯誤] 語音辨識失敗或逾時"


relay_agent = SttRelayAgent()
relay_agent.start_thread()


async def speech_to_text(file_path: str) -> str:
    return await relay_agent.request_stt(file_path)


@ui.page('/')
def main():
    ui.label('請上傳音檔進行語音辨識（STT）').classes('text-h5')

    result_area = ui.textarea(label='轉譯結果', placeholder='等待音檔處理中...').props('readonly').classes('w-full h-40')

    def on_upload(e):
        result_area.value = '處理中...'

        save_path = f'temp/{e.name}'
        with open(save_path, 'wb') as f:
            f.write(e.content.read())

        async def process():
            text = await speech_to_text(save_path)
            result_area.value = text

        asyncio.create_task(process())

    ui.upload(on_upload=on_upload, auto_upload=True, label='上傳音檔').classes('my-4')


ui.run(port=8088, title='TunaTalk 語音辨識', host='127.0.0.1')
