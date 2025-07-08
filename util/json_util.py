import json
import os
import logging

class JSONUtil:
    def save_to_json(data, filename):
        """
        데이터를 JSON 파일로 저장하는 함수 <br>
        data: 저장할 데이터 (딕셔너리 형식) <br>
        filename: 저장할 파일 이름
        """

        # 데이터가 없을 경우 저장하지 않음
        if not data:
            return
        
        # Ensure the output directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        try:
            with open(filename, 'w', encoding='utf-8') as json_file:
                json.dump(data, json_file, ensure_ascii=False, indent=4)
        except IOError as e:
            logging.error(f"Failed to save data to JSON file: {e}")
