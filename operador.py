# -*- coding: utf-8 -*-
"""
Created on Tue Jan 24 09:53:38 2023

@author: cpinto
"""

class Operator:
    def __init__(self, nome_da_tarefa):
        self.nome_da_tarefa = nome_da_tarefa
        
    def execute(self):
        self.log()
        
    def log(self):
        print(f'log {self.nome_da_tarefa}')
