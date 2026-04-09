# -*- coding: utf-8 -*-
from __future__ import annotations

from fts_event_calendar_service import EventCalendarService

if __name__ == '__main__':
    path, payload = EventCalendarService().build_summary()
    print('完成：', path)
    print(payload.get('status'))
    print('event_rows =', payload.get('event_table_rows'))
