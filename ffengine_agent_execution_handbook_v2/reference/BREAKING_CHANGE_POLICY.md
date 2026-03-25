# Kırılıcı Değişiklik Yönetim Politikası

## 1. Amaç
Wave'ler arası geriye uyumluluk ve Community/Enterprise sınır bütünlüğünü koruma kuralları.
Bu doküman, hangi interface'lerin ne zaman dondurulduğunu ve değişiklik yapılması gerektiğinde izlenecek süreci tanımlar.

## 2. Dondurulmuş Arayüzler

Aşağıdaki interface'ler belirtilen wave sonrasında **değiştirilemez**, yalnızca **genişletilebilir** (yeni opsiyonel parametre/metot eklenebilir):

| Arayüz | Donma Wave'i | Referans |
|--------|-------------|----------|
| `BaseEngine` (run, is_available, detect) | Wave 2 sonrası | `reference/API_CONTRACTS.md` |
| `BaseDialect` (tüm abstract metotlar) | Wave 3 sonrası | `reference/API_CONTRACTS.md` |
| `ETLResult` dataclass alanları | Wave 2 sonrası | `reference/API_CONTRACTS.md` |
| Config YAML zorunlu alan seti | Wave 3 sonrası | `reference/CONFIG_SCHEMA.md` |
| Exception sınıf hiyerarşisi | Wave 5 sonrası | `reference/EXCEPTION_MODEL.md` |
| Logging zorunlu alanları | Wave 5 sonrası | `reference/LOGGING_SCHEMA.md` |
| Community Engine contracts (SourceReader, Streamer, TargetWriter, Transformer, ETLManager) | Wave 4 sonrası | `reference/API_CONTRACTS.md` |
| XCom summary anahtarları | Wave 5 sonrası | `reference/API_CONTRACTS.md` |

## 3. Kırılıcı Değişiklik Tanımları

| Tür | Örnek | Ciddiyet |
|-----|-------|----------|
| İmza değişikliği | Parametre silme, tip değiştirme, return type değişimi | KRİTİK |
| Davranış değişikliği | Aynı input'a farklı output, yan etki ekleme/kaldırma | YÜKSEK |
| Config alan silme/rename | `load_method` → `write_mode` | KRİTİK |
| Yeni zorunlu config alanı | Mevcut YAML'larda olmayan alan zorunlu yapma | ORTA |
| Exception sınıf silme/rename | `ConfigError` → `SettingsError` | YÜKSEK |
| Log alanı silme | Zorunlu log alanının kaldırılması | ORTA |

## 4. Değişiklik Onay Süreci

Dondurulmuş bir arayüzde değişiklik yapılması gerektiğinde:

```
1. ETKİ ANALİZİ üret:
   - Hangi epic'ler etkilenir?
   - Hangi dosyalar değişmeli?
   - Hangi testler kırılır?

2. GERİYE UYUMLULUK KATMANI tasarla:
   - Eski imzayı koruyan adapter/shim oluştur
   - Deprecation uyarısı ekle
   - Geçiş süresi belirle

3. MİGRASYON NOTU yaz:
   - Ne değişti?
   - Neden değişti?
   - Nasıl adapte edilmeli?

4. TÜM ETKİLENEN GATE TESTLERİNİ çalıştır:
   - Hem mevcut wave'in testleri
   - Hem bağımlı wave'lerin testleri

5. KULLANICI ONAYINI al:
   - Etki analizi + migrasyon notunu sun
   - Onay olmadan değişiklik uygulanmaz
```

## 5. Community → Enterprise Koruma Kuralları

| Kural | Açıklama |
|-------|----------|
| Import koruması | Enterprise eklentisi Community import'larını **kırmaz** |
| Config uyumu | Enterprise'a yeni config alanı eklendiğinde Community path'inde varsayılan değer ile çalışır |
| Engine fallback | `BaseEngine.detect("auto")` her zaman Community fallback verir; Enterprise yoksa hata üretmez |
| Çift gate testi | Ortak katman değişikliği yapıldığında **hem Community hem Enterprise** gate testleri çalıştırılır |
| Dialect genişletme | Enterprise'a özgü dialect metotları (ör. `generate_bulk_extract_query`) opsiyonel kalır |
| Config parse | Community, Enterprise-only config alanlarını okuyabilir ama kullanmaz; hata vermez |

## 6. Genişletilebilirlik Kuralları

Dondurulmuş arayüzlere aşağıdaki eklemeler **kırılıcı değişiklik sayılmaz**:

- Yeni **opsiyonel** parametre ekleme (varsayılan değer ile)
- Yeni metot ekleme (ABC'ye abstract olmayan)
- Yeni exception alt sınıfı ekleme (mevcut hiyerarşi korunursa)
- Yeni opsiyonel config alanı ekleme (varsayılan değer ile)
- Yeni opsiyonel log alanı ekleme
- ETLResult'a yeni opsiyonel alan ekleme

## 7. Agent Kontrol Kuralları

Agent, kod üretirken şunları otomatik kontrol eder:

1. Değiştirilen dosya dondurulmuş bir interface içeriyor mu? → `§2 tablosunu` kontrol et
2. Config'e yeni zorunlu alan ekleniyor mu? → `§3 ciddiyet tablosunu` kontrol et
3. Exception sınıfı rename/silme yapılıyor mu? → `§3 ciddiyet tablosunu` kontrol et
4. Community modülüne Enterprise import'u ekleniyor mu? → `§5 koruma kurallarını` kontrol et

Herhangi biri tetiklenirse → `reference/AGENT_DECISION_TREE.md` K13 kuralı devreye girer: **DURDUR**, kırılıcı değişiklik onay sürecini başlat.
