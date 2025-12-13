from crm_parser import CRMParser

html = '''<tr class="           bg-status-1 bg-reqtype-10 bg-status-awaitOnly   " data-sortkey="s_1765334082" data-key="0"><td class="col__id pos-r"><a class="--blank-link" href="/admin/domain/customer-request/update?id=2129783" data-pjax="0" target="_blank">2129783 <sup><i class="fa fa-external-link-alt"></i></sup></a></td><td class="col__date col__openedAt col__req_status_awaitOnly pos-r zi-0"><div class="time-warning"></div><span class=" " title="Сейчас 18:46, Назначено в: 16:00(UTC)">12.12.25 19:00</span></td><td class="col__req_type">Впервые</td><td class="col__req_status">Ожидает</td><td class="--fullwidth"><a href="/admin/domain/customer-request/update?id=2129783" data-pjax="0">Волгоград</a></td><td class="col__phone"><span title="" style="white-space: nowrap">+7 960-***-5255</span></td><td><span style="min-width: 200px; max-width: 300px; display: block">улица Таращанцев, , 37</span></td><td><span title="В городе - 10.12.2025 12:34">10.12.25 19:34 (12:34)</span></td><td></td><td><span style="min-width: 120px; max-width: 180px; display: block">Топильский Д Е</span></td></tr>'''

parser = CRMParser()
items = parser.parse_requests_from_html(html)
print(items)
