import asyncio
import json
import re

from agentflow.core.agent import Agent

import logging
from app_logger import init_logging
logger:logging.Logger = init_logging()


class EventData:
    def __init__(self):
        self.data = None
        self.event = asyncio.Event()

    def set_data(self, data):
        self.data = data
        self.event.set()

    async def wait_for_data(self):
        await self.event.wait()
        return self.data


class Queryer(Agent):
    def __init__(self, name, agent_config):
        super().__init__(name, agent_config)
        self.connected_event = asyncio.Event()


    def on_connected(self):
        self.connected_event.set()


    async def query(self, question):
        await self.connected_event.wait()
        self.publish_sync("km.query.data", question)
        self.result_event = EventData()
        await self.result_event.wait_for_data()
        return self.result_event.data
    
    
    def clean_keyword(keyword):
        def remove_excluded_characters(input_string):
            characters_to_remove = "~!`@#$%^*_=\\><\/?'\"[]{}：；，‧"
            translation_table = str.maketrans('', '', characters_to_remove)
            cleaned_string = input_string.translate(translation_table)
            return cleaned_string
        
        keyword = remove_excluded_characters(keyword)
        keyword = re.sub(r'\s+', ' ', keyword).strip()
        keyword = keyword.replace('＆', '&').replace('　', '&')

        # Define a function to handle each part
        def process_part(part):
            if '(' in part and ')' in part:
                return part # Treat (expression) as a single operand
            else:
                return re.sub(r'\s+', '&', part.strip())    # Replace spaces within the part with &

        # Split the keyword into parts considering (expression) as a single operand
        parts = re.split(r'([&+\-()])', keyword)
        
        # Process each part and join them
        processed_parts = []
        for part in parts:
            if part in ['&', '+', '-', '(', ')']:
                processed_parts.append(part)
            else:
                processed_parts.append(process_part(part))

        # Join processed parts, adding & between operands if no operator is present
        cleaned_string = ''
        for i, part in enumerate(processed_parts):
            if i > 0 and part not in ['&', '+', '-', '(', ')'] and cleaned_string[-1] not in ['&', '+', '-', '(', ')']:
                cleaned_string += '&'
            cleaned_string += part
            
        cleaned_string = re.sub(r'(?<![&+\-])\(', r'&(', cleaned_string).lstrip('&')    # aaa( --> aaa&(
        cleaned_string = re.sub(r'\)(?![&+\-])', r')&', cleaned_string).rstrip('&')     # )aaa --> )&aaa

        def remove_redundant_parentheses(s):
            while True:
                new_s = re.sub(r'\(\((.*?)\)\)', r'(\1)', s)
                if new_s == s:
                    break
                s = new_s
            return s
        
        return remove_redundant_parentheses(cleaned_string)


    def extract_first_operand(expression):
        def find_first_operator(exp):
            # This function finds the first operator outside any parentheses
            stack = []
            for i, char in enumerate(exp):
                if char == '(':
                    stack.append('(')
                elif char == ')':
                    if stack:
                        stack.pop()
                elif char in '+-&' and not stack:
                    return i
            return -1

        expression = expression.strip()
        
        if expression[0] == '(' and expression[-1] == ')':
            # If the entire expression is a sub-expression, remove the outer parentheses
            expression = expression[1:-1].strip()
        
        operator_index = find_first_operator(expression)
        if operator_index != -1:
            first_operand = expression[:operator_index].strip()
            operator = expression[operator_index]
            remaining_expression = expression[operator_index+1:].strip()
            return first_operand, operator, remaining_expression
        
        return expression, None, ""
    
    
    def extract_last_operand(expression):
        def find_last_operator(exp):
            stack = []
            for i in range(len(exp) - 1, -1, -1):
                if exp[i] == ')':
                    stack.append(')')
                elif exp[i] == '(':
                    stack.pop()
                elif not stack:
                    if exp[i] in '+-&':
                        return i
            return -1

        # Remove outer parentheses if the entire expression is enclosed
        if expression[0] == '(' and expression[-1] == ')':
            expression = expression[1:-1]
        
        # Find the position of the last operator
        last_op_index = find_last_operator(expression)
        
        if last_op_index == -1:
            # If no operator found, the entire expression is a single operand
            return expression, "", ""
        
        # Extract remaining expression, operator, and last operand
        remaining_expression = expression[:last_op_index].strip()
        last_operator = expression[last_op_index]
        last_operand = expression[last_op_index + 1:].strip()
        
        return remaining_expression, last_operator, last_operand


    def union_nodes(nodes1, nodes2):
        id_map = {}

        for node in nodes1:
            id_map[node['id']] = node
        
        for node in nodes2:
            id_map[node['id']] = node
        
        return list(id_map.values())
    
    
    def diff_nodes(nodes1, nodes2):
        logger.debug(f"nodes1: {len(nodes1)}, nodes2: {len(nodes2)}")

        diff_ids = set()
        for node in nodes2:
            if 'properties' in node:
                if 'file_id' in node['properties']:
                    diff_ids.update(node['properties']['file_id'])
        # logger.debug(f"diff_ids: {len(diff_ids)}")
                
        for node in nodes1:
            if 'properties' in node:
                props = node['properties']
                if 'file_id' in props:
                    ids = props['file_id']
                    node['properties']['file_id'] = [id for id in ids if id not in diff_ids]
                if 'files' in props:
                    files = props['files']
                    node['properties']['files'] = [f for f in files if f['file_id'] not in diff_ids]
                    
        return nodes1
    
    
    def intersect_nodes(nodes1, nodes2):
        logger.debug(f"nodes1: {len(nodes1)}, nodes2: {len(nodes2)}")

        nodes2_ids = set()
        for node in nodes2:
            if 'properties' in node:
                if 'file_id' in node['properties']:
                    nodes2_ids.update(node['properties']['file_id'])
        # logger.debug(f"nodes2_ids: {len(nodes2_ids)}")
                
        for node in nodes1:
            if 'properties' in node:
                props = node['properties']
                if 'file_id' in props:
                    ids = props['file_id']
                    node['properties']['file_id'] = [id for id in ids if id in nodes2_ids]
                if 'files' in props:
                    files = props['files']
                    node['properties']['files'] = [f for f in files if f['file_id'] in nodes2_ids]
                    
        return nodes1


    async def query_keyword(self, keyword):
        keyword = Queryer.clean_keyword(keyword)
        remainding, op, rear = Queryer.extract_last_operand(keyword)
        logger.debug(f"{remainding}, {op}, {rear}")
        if op:
            q1 = Queryer(self.config)
            q2 = Queryer(self.config)
            q1.start_thread()
            q2.start_thread()
            answer1, answer2 = await asyncio.gather(
                q1.query_keyword(remainding),
                q2.query_keyword(rear))
            q1.terminate()
            q2.terminate()
            if op == '+':
                answer = Queryer.union_nodes(answer1, answer2)
            elif op == '-':
                answer = Queryer.diff_nodes(answer1, answer2)
            else:   # op == '&'
                answer = Queryer.intersect_nodes(answer1, answer2)
            return answer
        else:
            logger.debug(f"keyword: {keyword}")
            await self.connected_event.wait()
            self.query_logistic.publish("km.query.keyword", keyword)
            self.result_event = EventData()
            await self.result_event.wait_for_data()
            answer = self.result_event.data
            return answer['nodes']


    # async def query_keyword(self, keyword):
    #     keyword = Queryer.clean_keyword(keyword)
    #     first, op, remainding = Queryer.extract_first_operant(keyword)
    #     logger.debug(f"{first}, {op}, {remainding}")
    #     if op:
    #         q1 = Queryer(self.config)
    #         q2 = Queryer(self.config)
    #         q1.start_thread()
    #         q2.start_thread()
    #         answer1, answer2 = await asyncio.gather(
    #             q1.query_keyword(first),
    #             q2.query_keyword(remainding))
    #         q1.terminate()
    #         q2.terminate()
    #         if op == '+':
    #             answer = Queryer.union_nodes(answer1, answer2)
    #         elif op == '-':
    #             answer = Queryer.diff_nodes(answer1, answer2)
    #         else:   # op == '&'
    #             answer = Queryer.intersect_nodes(answer1, answer2)
    #         return answer
    #     else:
    #         logger.debug(f"keyword: {keyword}")
    #         await self.connected_event.wait()
    #         self.query_logistic.publish("km.query.keyword", keyword)
    #         self.result_event = EventData()
    #         await self.result_event.wait_for_data()
    #         answer = self.result_event.data
    #         return answer['nodes']


    def handle_km_query_result(self, topic:str, payload):
        # logger.debug(f"topic: {topic}, payload: {payload}")
        answer = json.loads(payload) #.decode('utf-8', 'ignore')
        self.result_event.set_data(answer)
        self.terminate()


# class AndQueryer(Queryer):
#     def __init__(self, cfg:AbdiConfig):
#         super().__init__(cfg)
#         self.left_nodes = None
#         self.right_nodes = None


#     async def query_keyword(self, keyword):
#         left_keyword, right_keyword = split_keyword(keyword)
#         self.query_logistic.publish("km.query.keyword", left_keyword)
#         self.query_logistic.publish("km.query.keyword", right_keyword)


#     def handle_km_query_result(self, topic:str, payload):
#         logger.debug(f"topic: {topic}, payload: {payload}")
#         answer = payload #.decode('utf-8', 'ignore')
#         self.result_event.set_data(answer)
#         self.terminate()
