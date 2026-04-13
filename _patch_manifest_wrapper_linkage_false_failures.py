# -*- coding: utf-8 -*-
"""
更新檔說明：
- 修正 fts_project_healthcheck.py 對 retired wrappers 的誤判。
- screening.py 與 master_pipeline.py 在新架構中屬於 optional/retired wrappers，
  只要新 service 存在就不再計為 wrapper_linkage_failure。
- 不影響其他 required wrappers 的檢查邏輯。
"""
