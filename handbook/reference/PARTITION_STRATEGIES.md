# Partition Strategies

- `auto_numeric`: numeric PK veya artan kolon üstünden min/max aralık bölme
- `percentile`: veri dağılımı dengesizse yaklaşık eşit hacim için percentile tabanlı bölme
- `hash_mod`: key alanı üzerinde hash modül parçalama
- `distinct`: ayrık değer listesi bazlı bölme
- `explicit`: kullanıcı tanımlı WHERE listesi
- `full_scan`: partition yok, tek parça çalıştırma

## Çıktı Formatı
```python
[{"part_id": 0, "where": "id >= 1 AND id < 100000"}]
```

## Kullanım
Partition planlama her zaman DAG Faz 1'de yapılır ve Dynamic Task Mapping'e XCom ile verilir.
