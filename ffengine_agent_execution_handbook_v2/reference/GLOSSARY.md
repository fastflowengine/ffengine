# Glossary

- **Engine-Swap**: Community Python Engine ile Enterprise C Engine'in aynı BaseEngine kontratını paylaşması
- **Query-Centric**: Cursor/fetchmany tabanlı veri taşıma modeli
- **Queue-Aware**: Ingress/Egress queue ve ack semantiği ile yönetilen veri taşıma modeli
- **FFEnvelope**: Enterprise queue runtime içinde metadata + payload sarmalayıcı nesne
- **CheckpointStore**: Enterprise restart/resume durumu için offset ve ack kayıt deposu
- **DLQ**: Başarısız mesajların yönlendirildiği dead-letter queue veya tablo
- **Dynamic Task Mapping**: Airflow'da partition spec listesinin paralel task'lara genişletilmesi
- **Binding Resolver**: `source`, `target`, `literal`, `airflow_var` değerlerini resolve eden katman
- **Mapping File**: `column_mapping_mode: mapping_file` kullanıldığında kolon eşlemesini tanımlayan YAML dosyası
