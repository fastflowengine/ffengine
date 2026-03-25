# ETL Studio UI Plugin Pattern

## Amaç
Apache Airflow'un Admin menüsüne "ETL Studio" eklentisini Airflow Plugin API ile eklemek.

## Kısıt
UI katmanı her iki versiyonda ortaktır. Enterprise özellikler UI'da görünebilir,
ancak runtime desteği scope'a göre kontrol edilmelidir.

---

## Plugin Kayıt Yapısı
```python
# src/ffengine/ui/plugin.py
from airflow.plugins_manager import AirflowPlugin
from airflow.www.app import csrf
from flask import Blueprint
from flask_appbuilder import expose, has_access, BaseView

etl_studio_bp = Blueprint(
    "etl_studio",
    __name__,
    template_folder="templates",
    static_folder="static",
    static_url_path="/static/etl_studio",
)

class ETLStudioView(BaseView):
    default_view = "index"

    @expose("/")
    @has_access
    def index(self):
        return self.render_template("etl_studio/index.html")

    @expose("/schemas")
    @has_access
    def schemas(self):
        """Kaynak DB'den şema listesi döner. Maksimum 50 tablo."""
        ...

    @expose("/tables")
    @has_access
    def tables(self):
        """Seçilen şemadan tablo listesi. Typeahead arama destekler."""
        ...

    @expose("/create-dag", methods=["POST"])
    @has_access
    @csrf.exempt
    def create_dag(self):
        """YAML config + DAG dosyası üretir."""
        ...

    @expose("/update-dag", methods=["POST"])
    @has_access
    @csrf.exempt
    def update_dag(self):
        """Mevcut DAG'ı günceller. Yalnızca UI ile üretilmiş DAG'lar güncellenebilir."""
        ...

    @expose("/timeline")
    @has_access
    def timeline(self):
        """Çalışan DAG'ların timeline görünümü."""
        ...

etl_studio_view = ETLStudioView()

etl_studio_package = {
    "name": "ETL Studio",
    "category": "Admin",
    "view": etl_studio_view,
}

class ETLStudioPlugin(AirflowPlugin):
    name = "etl_studio_plugin"
    flask_blueprints = [etl_studio_bp]
    appbuilder_views = [etl_studio_package]
```

## Ekranlar ve Davranışlar

| Ekran | Yol | Davranış |
|---|---|---|
| Ana ekran | `/etl_studio/` | Aktif DAG özeti, hızlı işlem butonları |
| Şema keşfi | `/etl_studio/schemas` | Seçili connection'daki şema listesi |
| Tablo keşfi | `/etl_studio/tables?schema=X&q=Y` | 50 tablo limiti; `q` ile typeahead filtre |
| DAG oluştur | `/etl_studio/create-dag` | YAML + DAG dosyası üretir; SQL ayrı `.sql` dosyasına yazılır |
| DAG güncelle | `/etl_studio/update-dag` | Mevcut config güncellenir; DAG dosyası yeniden üretilir |
| Timeline | `/etl_studio/timeline` | Airflow DagRun API üzerinden aktif çalışmalar |

## Şema Keşfi Kuralları
- Maksimum 50 tablo döner; fazlası sayfalanır
- Arama filtresi harf girildiğinde tetiklenir (typeahead — minimum 2 karakter)
- Kolon metadata: ad, tip, nullable, precision, scale
- Büyük tablolarda (>1M satır) satır sayısı tahmini gösterilir, kesin değil

## DAG Oluşturma Kuralları
- `task_group_id` → `{src_schema}_{src_table}_to_{tgt_schema}_{tgt_table}_v1` formatında otomatik üretilir
- SQL sorgusu varsa `sql/` dizinine ayrı dosya olarak yazılır; `config.yaml`'da `sql_file:` ile referans verilir
- Tag'ler dizin yolundan otomatik türetilir: `{domain}/{level}/{direction}`
- Üretilen dosyalar `projects/{proje}/{domain}/level{N}/{yön}/` dizinine yazılır

## Tag Yönetimi
```python
# Dizin yapısından otomatik tag türetme
path = "projects/webhook/whk/level1/src_to_stg"
tags = ["whk", "level1", "src_to_stg"]

# UI'den ek tag eklenebilir; otomatik tag'ler silinmez
```

## Enterprise UI Notu
Enterprise özellikleri (queue depth gösterimi, throughput grafiği, multi-lane status)
UI'da render edilebilir ancak sayfa render öncesinde şu kontrol yapılmalıdır:
```python
from ffengine.core.engine_interface import BaseEngine
is_enterprise = BaseEngine.detect("auto").is_available() and \
                type(BaseEngine.detect("auto")).__name__ == "CEngine"
```

## Agent Kontrol Listesi
1. Plugin `AirflowPlugin` sınıfından mı türetiliyor?
2. `appbuilder_views` ile Admin menüsüne eklendi mi?
3. Şema keşfi 50 tablo limitini uyguluyor mu?
4. DAG oluşturma SQL'i ayrı dosyaya mı yazıyor?
5. Tag'ler dizin yolundan türetiliyor mu?
6. CSRF koruması POST endpoint'lerinde aktif mi?
7. Enterprise-only görünümler `is_enterprise` kontrolüne bağlı mı?