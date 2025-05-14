import json, os
import io
import sys
import time
import csv
import builtins
from ftplib import FTP
from flask import Flask, request, redirect, url_for, render_template_string, flash, Response
import httpx
import pandas as pd
from httpx import Timeout
import re
from datetime import datetime
from zoneinfo import ZoneInfo





# ——— Настраиваем “двойной” print — вывод в терминал и в буфер ———
buf_stdout = io.StringIO()
_original_print = builtins.print
def print(*args, **kwargs):
    # 1) в настоящий терминал
    _original_print(*args, file=sys.__stdout__, **{k:v for k,v in kwargs.items() if k not in ("file",)})
    # 2) в наш буфер
    sep = kwargs.get("sep", " ")
    end = kwargs.get("end", "\n")
    buf_stdout.write(sep.join(str(a) for a in args) + end)
builtins.print = print
# —————————————————————————————————————————————————————————————

app = Flask(__name__)
app.secret_key = os.urandom(24)
SYNC_IN_PROGRESS = False

# --- КОНФИГУРАЦИЯ ---
SHOP_NAME     = os.getenv('SHOPIFY_STORE_URL')
API_TOKEN     = os.getenv('SHOPIFY_ACCESS_TOKEN')
API_VERSION   = "2024-01"
LOCATION_ID   = os.getenv('LOCATION_ID')

FTP_HOST      = os.getenv('FTP_HOST')
FTP_USER      = os.getenv('FTP_USER')
FTP_PASS      = os.getenv('FTP_PASS')
FTP_FILE_PATH = "/csv_folder/TSGoods.trs"

POSSIBLE_OPTIONS = ["TheSize", "dlina_stelki", "objem_golenisha"]
MIN_INTERVAL     = 0.5
_last_call       = 0.0

EXCEL_PATH = os.path.join(os.path.dirname(__file__), "хорошоп.xlsx")

# дефолтные настройки
DEFAULT_SYNC_SETTINGS = {
    "update_price_qty": True,
    "update_sale_price": True,
    "update_description": True,
}

SETTINGS_FILE = os.path.join(os.path.dirname(__file__), 'sync_settings.json')


# Попытка загрузить сохранённые настройки из файла
try:
    saved = json.load(open(SETTINGS_FILE, 'r'))
    DEFAULT_SYNC_SETTINGS.update(saved)
except FileNotFoundError:
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(DEFAULT_SYNC_SETTINGS, f, indent=2)
except Exception as e:
    print("⚠️ Не удалось прочитать sync_settings.json:", e, flush=True)

app.config["SYNC_SETTINGS"] = DEFAULT_SYNC_SETTINGS.copy()


def load_excel_mapping():
    print(f"📥 Завантажуємо Excel-мапінг з {EXCEL_PATH}", flush=True)
    df = pd.read_excel(EXCEL_PATH, header=None, dtype=str, engine="openpyxl").fillna("")
    mapping = {}
    for _, row in df.iterrows():
        key = row[0].strip()
        if not key:
            continue
        title    = row[6].strip()
        img_cell = row[17].strip() or row[18].strip()
        urls     = [u.strip() for u in img_cell.replace("\n", ";").split(";") if u.strip()]
        mapping[key] = {"title": title, "images": urls}
    print(f"✅ Excel-мапінг завантажено: {len(mapping)} записей", flush=True)
    return mapping

EXCEL_MAP = load_excel_mapping()

def shopify_request(client: httpx.Client, method: str, url: str, max_retries: int = 3, **kwargs):

    global _last_call

    for attempt in range(1, max_retries + 1):
        # 1) throttle по MIN_INTERVAL
        now = time.time()
        if now - _last_call < MIN_INTERVAL:
            time.sleep(MIN_INTERVAL - (now - _last_call))

        resp = client.request(method, url, **kwargs)
        _last_call = time.time()

        # 2) rate-limit
        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "2"))
            print(f"⚠️ 429 от Shopify, ждём {retry_after}s… (попытка {attempt}/{max_retries})", flush=True)
            time.sleep(retry_after)
            continue

        # 3) продукт занят
        if resp.status_code == 409:
            try:
                errors = resp.json().get("errors", {}).get("product", [])
            except ValueError:
                errors = []
            if errors and errors[0].startswith("This product is currently being modified"):
                wait = 0.5 * attempt
                print(f"⚠️ Продукт занят, retry #{attempt} через {wait}s", flush=True)
                time.sleep(wait)
                continue

        # 4) всё остальное — возвращаем ответ
        return resp

    # если исчерпали все попытки, возвращаем последний ответ
    return resp

meta_columns = set()

DEFAULT_SYNC_SETTINGS = {
    "update_price_qty": True,
    "update_sale_price": True,
    "update_description": True,
}
app.config["SYNC_SETTINGS"] = DEFAULT_SYNC_SETTINGS.copy()


HOME_TEMPLATE = """
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <title>Імпорт у Shopify</title>
  <style>
    body { font-family: 'Segoe UI', Tahoma, sans-serif; background: #f0f2f5; margin:0; padding:0; }
    .container { max-width: 600px; margin:4em auto; background:#fff; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.05); padding:2em; }
    h1 { margin-bottom:1em; color:#333; }
    ul { list-style:none; padding:0; }
    li { margin:0.5em 0; }
    a.card {
      display:block; padding:1em 1.5em; background:#fafafa; 
      border-radius:6px; text-decoration:none; color:#333; 
      box-shadow:0 2px 6px rgba(0,0,0,0.03);
      transition:background .2s, transform .2s;
    }
    a.card:hover { background:#fff; transform:translateY(-2px); }
  </style>
</head>
<body>
  <div class="container">
    <h1>Імпорт у Shopify</h1>
    <ul>
      <li><a class="card" href="{{ url_for('settings') }}">⚙️ Настроювання та запуск синхронизації</a></li>
      <li><a class="card" href="{{ url_for('report') }}">📊 Звіт останньої синхронизації</a></li>
    </ul>
  </div>
</body>
</html>
"""

SETTINGS_TEMPLATE = """
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <title>Настроювання синхронизації</title>
  <style>
    body { font-family: 'Segoe UI', Tahoma, sans-serif; background: #f0f2f5; margin:0; padding:0; }
    .container { max-width: 700px; margin:3em auto; background:#fff; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.05); padding:2em; }
    h1 { color:#333; margin-bottom:0.5em; }
    fieldset { border:1px solid #e0e0e0; border-radius:6px; padding:1.2em; margin-bottom:1.5em; background:#fafafa; }
    legend { font-weight:bold; padding:0 0.5em; }
    input, button { font-size:1rem; }
    input[type="text"], input[type="checkbox"] + label { margin-right:0.5em; }
    input[type="text"] { padding:0.5em; border:1px solid #ccc; border-radius:4px; width:calc(100% - 1.2em); }
    button { padding:0.6em 1.2em; border:none; border-radius:4px; background:#3498db; color:#fff; cursor:pointer; transition:background .2s; }
    button:hover { background:#2980b9; }
    #overlay {
      display: none;
      position: fixed; top: 0; left: 0;
      width: 100%; height: 100%;
      background: rgba(0,0,0,0.3);
      z-index: 9999;
      align-items: center;
      justify-content: center;
      flex-direction: column;
    }
    .spinner {
      width:50px; height:50px; border:5px solid #ddd; border-top:5px solid #3498db;
      border-radius:50%; animation:spin 1s linear infinite;
    }
    @keyframes spin { to { transform:rotate(360deg); } }
    .flash { margin-top:1em; color:#27ae60; font-weight:bold; }
    a.back { text-decoration:none; color:#555; margin-bottom:1em; display:inline-block; }
  </style>
    <script>
    // Список заголовків CSV
    const csvFields = [
      'ModelGoodID','GoodID','Analogs','Articul','EqualCurrencyName','GoodTypeName','GoodTypeFull',
      'PCName','ProducerCollectionFull','Height','Display','Age','ProductionDate','Length',
      'EqualWholesalePrice','EqualSalePrice','PowerSupply','PriceDiscountPercent','Category',
      'WarehouseQuantity','WarehouseQuantityForPartner','SuppLierCode','Color','ShortName',
      'Country','CountUnitsPerBox','Material','MinQuantityForOrder','MinWarehouseQuantity',
      'WholesaleCount','Measure','GoodName','FashionName','MesUnit','GuaranteeMesUnit',
      'Description','WholesalePricePerUnit','SynchronizationSection','SynchronizationSectionFull',
      'TheSize','PackSize','Season','Sex','GuaranteePeriod','Pack','Closeout','GoodPhotoList',
      'GoodPhotoListWithLinks','RetailPriceWithDiscount','CurrencyPriceWholesale_4',
      'CurrencyPriceRetail_4','RetailPricePerUnit','WholesalePrice','RetailPrice','Width','Barcode',
      'CurrencyPriceWholesale_1','CurrencyPriceWholesale_3','CurrencyPriceRetail_1',
      'CurrencyPriceRetail_3','hight_low_top','vyd_zastibky','toe','visibility_on_site',
      'visota_kabluka','visota_platformi','visota_tanketki','visota_golenisha','dlina_stelki',
      'material_verha','material_podkaldki','material_podoshvi','volume_in_bundles',
      'objem_golenisha','pdgrupa','polnota','stil_obuvi','fason'
    ];

    document.addEventListener('DOMContentLoaded', () => {
      const input = document.getElementById('new_meta'),
            addBtn = document.getElementById('btn_add_meta'),
            err   = document.getElementById('meta_error');

      // при вводе проверяем
      input.addEventListener('input', () => {
        const val = input.value.trim();
        if (!val || csvFields.indexOf(val) === -1) {
          err.textContent = 'відсутнє поле з такою назвою';
          addBtn.disabled  = true;
        } else {
          err.textContent = '';
          addBtn.disabled  = false;
        }
      });
    });
  </script>
</head>
<body>
  <div class="container">
    <h1>Настроювання та запуск</h1>
    <a class="back" href="{{ url_for('home') }}">← На головну сторінку</a>

 <fieldset>
    <legend>1. Метафілди</legend>
    <form method="post" action="{{ url_for('settings') }}">
      <input
        id="new_meta"
        name="new_meta"
        placeholder="Назва колонки"
        required
        style="margin-right:0.5em;"
      >
      <button
        id="btn_add_meta"
        name="action"
        value="add_meta"
        disabled
      >Додати</button>
      <button
        name="action"
        value="clear_meta"
        style="background:#e74c3c; margin-left:0.5em;"
      >Видалити все</button>
      <div id="meta_error" class="error"></div>
    </form>

    {% if meta_columns %}
      <ul>
        {% for c in meta_columns %}
          <li>
            {{ c }}
            <form method="post" action="{{ url_for('settings') }}" style="display:inline">
              <input type="hidden" name="meta_to_delete" value="{{ c }}">
              <button
                name="action"
                value="delete_meta"
                style="
                  background:#e74c3c;
                  border:none;
                  color:#fff;
                  padding:0 0.3em;
                  margin-left:0.5em;
                  cursor:pointer;
                "
                title="Видалити {{ c }}"
              >×</button>
            </form>
          </li>
        {% endfor %}
      </ul>
    {% else %}
      <p>Порожньо</p>
    {% endif %}
  </fieldset>

<fieldset>
  <legend>2. Налаштування синхронізації</legend>

  <div style="display:inline-grid; gap:0.5em; margin-bottom:1em;">
    <label>
      <input type="checkbox" name="update_price_qty"
             {% if sync_settings.update_price_qty %}checked{% endif %}>
      Оновити ціну та залишки
    </label>
    <label>
      <input type="checkbox" name="update_sale_price"
             {% if sync_settings.update_sale_price %}checked{% endif %}>
      Оновити розпродажну ціну
    </label>
    <label>
      <input type="checkbox" name="update_description"
             {% if sync_settings.update_description %}checked{% endif %}>
      Оновити опис
    </label>
  </div>

  {# Наша новая кнопка #}
  <div style="margin-top:1em;">
    <button
      id="importBtn"
      type="button"
      style="
        background: #3498db;
        color: #fff;
        border: none;
        border-radius: 4px;
        padding: 0.6em 1.2em;
        cursor: pointer;
      "
    >
      ⚙️ Запустити синхронізацію
    </button>
  </div>

</fieldset>

<!-- сам оверлей и спиннер (должен быть внизу страницы, рядом с <body>) -->
<div id="overlay" style="
     display: none;
     position: fixed; top: 0; left: 0;
     width: 100%; height: 100%;
     background: rgba(0,0,0,0.3);
     z-index: 9999;
     align-items: center;
     justify-content: center;
">
  <div class="spinner" style="
       width:50px; height:50px;
       border:5px solid #ddd;
       border-top:5px solid #3498db;
       border-radius:50%;
       animation: spin 1s linear infinite;
  "></div>
  <p style="color:#fff; margin-top:1em;">Проходить синхронізація… Зачекайте</p>
</div>

<style>
  @keyframes spin { to { transform: rotate(360deg); } }
</style>


    {% with msgs = get_flashed_messages() %}
      {% if msgs %}
        <div class="flash">
          {% for m in msgs %}<p>{{ m }}</p>{% endfor %}
        </div>
      {% endif %}
    {% endwith %}
  </div>

  <div id="overlay">
    <div class="spinner"></div>
    <p style="color:#fff; margin-top:1em;">Проходить синхронізація… Зачекайте</p>
  </div>

<script>
document.addEventListener('DOMContentLoaded', () => {
  const btn     = document.getElementById('importBtn');
  const overlay = document.getElementById('overlay');

  btn.addEventListener('click', () => {
    overlay.style.display = 'flex';

    const body = new URLSearchParams();
    body.set('action', 'import');

    fetch("{{ url_for('settings') }}", {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: body.toString()
    })
    .then(resp => {
      if (resp.redirected) {
        window.location.href = resp.url;
      } else {
        window.location.reload();
      }
    })
    .catch(err => {
      overlay.style.display = 'none';
      console.error(err);
      alert('❌ Помилка синхронізації: ' + err.message);
    });
  });
});
</script>
<script>
document.querySelectorAll('input[type=checkbox]').forEach(cb=>{
  cb.addEventListener('change', () => {
    fetch('{{ url_for("save_settings") }}', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        key: cb.name,
        value: cb.checked
      })
    });
  });
});
</script>
<script>
  // если при рендере sync_in_progress == true —
  // сразу показываем overlay
  document.addEventListener('DOMContentLoaded', () => {
    const inProgress = {{ sync_in_progress|lower }};
    if (inProgress) {
      document.getElementById('overlay').style.display = 'flex';
    }
  });
</script>
</body>
</html>
"""

REPORT_TEMPLATE = """
<!doctype html>
<html lang="uk">
<head>
  <meta charset="utf-8">
  <title>Звіт синхронизації</title>
  <style>
    body { font-family: 'Segoe UI', Tahoma, sans-serif; background:#f0f2f5; margin:0; padding:0; }
    .container { max-width:800px; margin:3em auto; background:#fff; border-radius:8px; box-shadow:0 4px 12px rgba(0,0,0,0.05); padding:2em;}
    h1 { color:#333; margin-bottom:0.5em; }
    a.back { text-decoration:none; color:#555; margin-bottom:1em; display:inline-block; }
    pre { background:#fafafa; border:1px solid #eee; border-radius:6px; padding:1em; overflow:auto; max-height:70vh; }
  </style>
</head>
<body>
  <div class="container">
    <h1>Звіт по останній синхронизації</h1>
    <a class="back" href="{{ url_for('home') }}">← На головну сторінку</a>

    <h2>Логи виконання:</h2>
    <pre>
{% for line in logs %}
{{ line }}
{% endfor %}
    </pre>
  </div>
</body>
</html>
"""



@app.route("/", methods=["GET"])
def home():
    # Домашняя страница
    return render_template_string(HOME_TEMPLATE)


@app.route("/report", methods=["GET"])
def report():
    # получаем список строк лога (или заглушку, если его нет)
    logs = app.config.get("LAST_LOGS",
             ["(Логи ще не зібрані; спочатку натисніть «Запустити» на сторінці налаштувань або зачекайте, поки синхронізація завершиться.)"])
    return render_template_string(REPORT_TEMPLATE, logs=logs)

def fetch_file_from_ftp():
    print("🔄 Завантажуємо CSV по FTP…", flush=True)
    try:
        ftp = FTP(FTP_HOST); ftp.login(FTP_USER, FTP_PASS)
        buf = io.BytesIO()
        ftp.retrbinary(f"RETR {FTP_FILE_PATH}", buf.write)
        ftp.quit()
        raw = buf.getvalue()
        print(f"✅ Прочитано {len(raw)} байт", flush=True)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw.decode("cp1251")
    except Exception as e:
        print("❌ Помилка FTP:", e, flush=True)
        return None

@app.route('/settings/save', methods=['POST'])
def save_settings():
    data = request.get_json()
    # читаем текущее
    try:
        cfg = json.load(open(SETTINGS_FILE))
    except FileNotFoundError:
        cfg = {}
    # обновляем
    cfg[data['key']] = bool(data['value'])
    # сохраняем
    with open(SETTINGS_FILE, 'w') as f:
        json.dump(cfg, f)
    return '', 204

@app.route("/settings", methods=["GET", "POST"])
def settings():
    global SYNC_IN_PROGRESS
    view = request.args.get("view", "sync")
    if request.method == "GET":
        sync_settings = app.config.get("SYNC_SETTINGS", DEFAULT_SYNC_SETTINGS)
        return render_template_string(
            SETTINGS_TEMPLATE,
            meta_columns=sorted(meta_columns),
            sync_settings=sync_settings,  # <-- вот его и передаём
            sync_in_progress=SYNC_IN_PROGRESS,  # <-- передаём флаг
            view=view
        )


    act = request.form["action"]
    if act == "add_meta":
        meta_columns.add(request.form["new_meta"].strip())
        flash("Додан метафілд")
        return redirect(url_for("settings"))
    if act == "delete_meta":
        to_del = request.form.get("meta_to_delete", "").strip()
        if to_del in meta_columns:
            meta_columns.remove(to_del)
            flash(f"Метафілд «{to_del}» видалено")
        return redirect(url_for("settings"))

    if act == "save_settings":
        # обновляем app.config и файл
        cfg = json.load(open(SETTINGS_FILE, 'r'))
        for key in ("update_price_qty", "update_sale_price", "update_description"):
            cfg[key] = key in request.form
        with open(SETTINGS_FILE, 'w') as f:
            json.dump(cfg, f, indent=2)
        app.config["SYNC_SETTINGS"].update(cfg)
        flash("Налаштування збережені")
        return redirect(url_for("settings"))


    if act == "clear_meta":
        meta_columns.clear()
        flash("Метафілди видалені")
        return redirect(url_for("settings"))


    if act == "import":
        SYNC_IN_PROGRESS = True

        ua_now = datetime.now(ZoneInfo("Europe/Kyiv"))
        print(ua_now.strftime("%Y-%m-%d %H:%M:%S %Z"), "🔄 Старт синхронізації")

        # Сбрасываем буфер перед запуском
        buf_stdout.truncate(0)
        buf_stdout.seek(0)

        # сохраняем, как пользователь поставил чекбоксы
        sync_settings = app.config["SYNC_SETTINGS"]
        upd = sync_settings["update_price_qty"]
        upd_sale = sync_settings["update_sale_price"]
        upd_desc = sync_settings["update_description"]

        txt = fetch_file_from_ftp()
        if not txt:
            flash("Немає файлу Торгсофт", "error")
            app.config["LAST_LOGS"] = buf_stdout.getvalue().splitlines()
            return redirect(url_for("settings"))

        reader = csv.reader(io.StringIO(txt), delimiter=";")
        header = next(reader)
        idx    = {h:i for i,h in enumerate(header)}
        rows   = list(reader)

        # группируем по Articul
        groups = {}
        for r in rows:
            sku = r[idx["Articul"]].strip()
            groups.setdefault(sku, []).append(r)
        print(f"🔑 Всього SKU-груп: {len(groups)}", flush=True)

        created = updated = 0

        sync_settings = {
            "update_price_qty": True,
            "update_sale_price": True,
            "update_description": True,
        }

        with httpx.Client(verify=False, timeout=Timeout(120, connect=10)) as client:
            client.headers.update({
                "X-Shopify-Access-Token": API_TOKEN,
                "Content-Type": "application/json"
            })

            seen_handles = set()

            for sku, recs in groups.items():
                print(f"\n▶ Articul={sku}, variants={len(recs)}", flush=True)
                if not upd_desc:
                    print("    ⚠️ Оновлення опису вимкнено — опис залишиться без змін", flush=True)
                if not upd_sale:
                    print("    ⚠️ Оновлення розпродажної ціни вимкнено — буде використана тільки стандартна ціна",
                          flush=True)

                goodid = recs[0][idx["GoodID"]]
                key = f"{sku}-{goodid}"
                xl = EXCEL_MAP.get(key)
                print(f"  • Excel[{key}]:", "є" if xl else "немає", flush=True)

                # title / description / images
                title = xl["title"] if xl and xl["title"] else recs[0][idx["Description"]]
                description = recs[0][idx["Description"]]
                images = xl["images"] if xl else []

                # базовые данные
                vendor = recs[0][idx["ProducerCollectionFull"]].strip()
                country_meta = recs[0][idx["Country"]].strip()
                season = recs[0][idx["Season"]]
                raw_title = xl["title"] if xl and xl["title"] else recs[0][idx["Description"]]
                t = raw_title.lower()
                slug_title = re.sub(r'[^\w\-]+', '-', t, flags=re.UNICODE).strip('-')
                handle = f"{slug_title}-{sku.lower().replace(' ', '-')}"
                status = "active" if recs[0][idx["visibility_on_site"]] == "1" else "draft"
                tags = recs[0][idx["GoodTypeFull"]]

                if handle in seen_handles:
                    print(f"⚠️ Пропускаємо дублікат — handle={handle} вже оброблений", flush=True)
                    continue
                seen_handles.add(handle)

                # --- собираем базовые metafields ---
                mf = [
                    {"namespace": "custom", "key": "country", "value": country_meta, "type": "single_line_text_field"},
                    {"namespace": "custom", "key": "season", "value": season, "type": "single_line_text_field"},
                ]
                for c in meta_columns:
                    if c in idx:
                        v = recs[0][idx[c]].strip()
                        mf.append({"namespace": "custom", "key": c, "value": v, "type": "single_line_text_field"})
                print(f"    ⇒ mf-ключі до last_size: {[m['key'] for m in mf]}", flush=True)

                # last_size
                active_sizes = [r[idx["TheSize"]].strip() for r in recs if int(r[idx["WarehouseQuantity"]]) > 0]
                print(f"    ℹ️ Активні розміри: {active_sizes}", flush=True)
                if len(active_sizes) == 1:
                    mf.append({
                        "namespace": "custom", "key": "last_size", "value": active_sizes[0],
                        "type": "single_line_text_field"
                    })
                    print(f"    ✨ Додаємо metafield last_size='{active_sizes[0]}'", flush=True)

                # --- options & variants ---
                opt_cols = [c for c in POSSIBLE_OPTIONS if c in idx and all(r[idx[c]].strip() for r in recs)]
                options = [{"name": c, "values": sorted({r[idx[c]].strip() for r in recs})} for c in opt_cols]
                print(f"  • Опції: {opt_cols}", flush=True)

                variants = []
                for r in recs:
                    v = {"sku": sku, "barcode": r[idx["Barcode"]]}
                    for i, c in enumerate(opt_cols, 1):
                        v[f"option{i}"] = r[idx[c]].strip()
                    if upd:
                        retail = r[idx["RetailPrice"]].strip()
                        disc = r[idx["RetailPriceWithDiscount"]].strip()
                        try:
                            rf = float(retail)
                            df = float(disc) if disc else rf
                        except:
                            rf = df = rf

                        v["price"] = str(rf)

                        if upd_sale and disc and df < rf:
                            v["price"] = str(df)
                            v["compare_at_price"] = str(rf)
                            print(f"    💲 Додаємо ціну зі зніжкою: price={disc}, compare_at_price={retail}",
                                  flush=True)
                        else:
                            v["price"] = retail
                            print(f"    💲 Додаємо звичайну ціну: price={retail}", flush=True)
                        v["inventory_management"] = "shopify"
                    variants.append(v)
                print(f"  • варіантів = {len(variants)}", flush=True)

                # --- payload ---
                payload = {"product": {
                    "title": title,
                    **({"body_html": description} if upd_desc else {}),
                    "vendor": vendor,
                    "handle": handle,
                    "status": status,
                    "tags": tags,
                    "options": options,
                    "variants": variants,
                    "metafields": mf
                }}
                if images:
                    payload["product"]["images"] = [{"src": u} for u in images]
                    print(f"  • Додаємо {len(images)} image(s)", flush=True)

                # --- GET по handle ---
                resp = shopify_request(client, "GET",
                                       f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products.json",
                                       params={"handle": handle})
                prods = resp.json().get("products", [])
                print(f"  • У Shopify: {len(prods)}", flush=True)

                if prods:
                    # UPDATE
                    pid = prods[0]["id"]
                    print(f"🛠️ Оновлюємо товар ID={pid}" + (" (цены/остатки)" if upd else ""), flush=True)
                    r2 = shopify_request(client, "PUT",
                                         f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products/{pid}.json",
                                         json=payload)
                    if r2.status_code in (200, 201):
                        updated += 1
                        print(f"    ✅ Товар ОНОВЛЕНО ({updated})", flush=True)

                        # GET существующих metafields
                        lst = shopify_request(client, "GET",
                                              f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products/{pid}/metafields.json",
                                              params={"namespace": "custom"})
                        existing = lst.json().get("metafields", []) if lst.status_code < 300 else []
                        print(f"    ℹ️ Існуючі MF та їхні значення: {[(m['key'], m['value']) for m in existing]}",
                              flush=True)
                        existing_map = {m["key"]: m for m in existing}

                        # --- Article из CSV ---
                        art = recs[0][idx["Articul"]].strip()
                        if "Article" in existing_map:
                            cur = existing_map["Article"]["value"]
                            print(f"    ℹ️ Поточне значення Article = '{cur}'", flush=True)
                            if not cur:
                                mid = existing_map["Article"]["id"]
                                res = shopify_request(client, "PUT",
                                                      f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products/{pid}/metafields/{mid}.json",
                                                      json={"metafield": {
                                                          "namespace": "custom", "key": "Article",
                                                          "value": art, "type": "single_line_text_field"
                                                      }})
                                if res.status_code < 300:
                                    print(f"    🔄 Article оновлено → '{art}'", flush=True)
                                else:
                                    print(f"    ❌ Помилка оновлення Article: {res.text}", flush=True)
                        else:
                            res = shopify_request(client, "POST",
                                                  f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products/{pid}/metafields.json",
                                                  json={"metafield": {
                                                      "namespace": "custom", "key": "Article",
                                                      "value": art, "type": "single_line_text_field"
                                                  }})
                            if res.status_code < 300:
                                print(f"    ✨ Створено Article → '{art}'", flush=True)
                            else:
                                print(f"    ❌ Помилка створення Article: {res.text}", flush=True)

                        # === остальные metafields ===
                        for mf_item in mf:
                            k = mf_item["key"]
                            if k in ("country", "season", "last_size", "Article"):
                                continue
                            if k in existing_map:
                                mid = existing_map[k]["id"]
                                res = shopify_request(client, "PUT",
                                                      f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products/{pid}/metafields/{mid}.json",
                                                      json={"metafield": mf_item})
                                if res.status_code < 300:
                                    print(f"    🔄 Metafield '{k}' оновлено", flush=True)
                            else:
                                res = shopify_request(client, "POST",
                                                      f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products/{pid}/metafields.json",
                                                      json={"metafield": mf_item})
                                if res.status_code < 300:
                                    print(f"    ✨ Metafield '{k}' створено", flush=True)


                    # === обновление остатков ===
                    if upd:
                        prod = r2.json().get("product", {})
                        variants = prod.get("variants") or prods[0].get("variants", [])

                        print("    🔄 Оновлюємо ціни та залишки:", flush=True)
                        for v in variants:
                            var_id = v["id"]
                            iid = v["inventory_item_id"]
                            opt1 = v.get("option1")
                            match = next((x for x in recs if opt_cols and x[idx[opt_cols[0]]] == opt1), recs[0])

                            # — 1) Ціна —
                            retail = match[idx["RetailPrice"]].strip()
                            disc = match[idx["RetailPriceWithDiscount"]].strip()
                            try:
                                rf = float(retail)
                                df = float(disc) if disc else rf
                            except:
                                rf = df = rf
                            new_price = df if (disc and df < rf) else rf

                            variant_payload = {"variant": {"id": var_id, "price": str(new_price)}}
                            if upd_sale and disc and df < rf:
                                variant_payload["variant"]["compare_at_price"] = str(rf)
                                print(f"      💲 Додаємо ціну зі знижкою: price={new_price}, compare_at_price={rf}",
                                      flush=True)
                            else:
                                print(f"      💲 Додаємо звичайну ціну: price={new_price}", flush=True)

                            # PUT на endpoint /variants/{id}.json
                            price_res = shopify_request(
                                client, "PUT",
                                f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/variants/{var_id}.json",
                                json=variant_payload
                            )
                            if price_res.status_code < 300:
                                print(f"      ✅ Variant {var_id} price updated", flush=True)
                            else:
                                print(f"      ❌ Помилка оновлення ціни variant_id={var_id}: {price_res.text}",
                                      flush=True)


                            q = int(match[idx["WarehouseQuantity"]])
                            inv_res = shopify_request(
                                client, "POST",
                                f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/inventory_levels/set.json",
                                json={"location_id": LOCATION_ID, "inventory_item_id": iid, "available": q}
                            )
                            if inv_res.status_code < 300:
                                print(f"      • option={opt1!r} → доступно={q}", flush=True)
                            else:
                                print(f"      ❌ Помилка оновлення залишків для option={opt1!r}: {inv_res.text}",
                                      flush=True)
                    else:
                        print("    ⚠️ Опція оновлення цін/залишків вимкнена", flush=True)




                else:

                    # CREATE новый товар

                    print("🚀 Створюємо новий товар", flush=True)

                    r2 = shopify_request(client, "POST",

                                         f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products.json",

                                         json=payload)

                    if r2.status_code in (200, 201):

                        created += 1

                        data = r2.json()

                        prod = data.get("product", {})

                        pid = prod.get("id")

                        variants = prod.get("variants", [])

                        # 1) Оновлюємо ціни та залишки

                        print("    🔄 Оновлюємо ціни та залишки для нових товарів:", flush=True)

                        for v in variants:

                            var_id = v["id"]

                            iid = v["inventory_item_id"]

                            opt1 = v.get("option1")

                            match = next(

                                (x for x in recs if opt_cols and x[idx[opt_cols[0]]] == opt1),

                                recs[0]

                            )

                            # — 1.1) Цена —

                            retail = match[idx["RetailPrice"]].strip()

                            disc = match[idx["RetailPriceWithDiscount"]].strip()

                            try:

                                rf = float(retail)

                                df = float(disc) if disc else rf

                            except:

                                rf = df = rf

                            new_price = df if (disc and df < rf) else rf

                            variant_payload = {"variant": {"id": var_id, "price": str(new_price)}}

                            if disc and df < rf:

                                variant_payload["variant"]["compare_at_price"] = str(rf)

                                print(f"      💲 Зі знижкою: price={new_price}, compare_at_price={rf}", flush=True)

                            else:

                                print(f"      💲 Без знижки: price={new_price}", flush=True)

                            price_res = shopify_request(

                                client, "PUT",

                                f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/variants/{var_id}.json",

                                json=variant_payload

                            )

                            if price_res.status_code < 300:

                                print(f"      ✅ Variant {var_id} price updated", flush=True)

                            else:

                                print(f"      ❌ Error updating price variant_id={var_id}: {price_res.text}", flush=True)

                            # — 1.2) Залишки —

                            q = int(match[idx["WarehouseQuantity"]])

                            inv_res = shopify_request(

                                client, "POST",

                                f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/inventory_levels/set.json",

                                json={"location_id": LOCATION_ID, "inventory_item_id": iid, "available": q}

                            )

                            if inv_res.status_code < 300:

                                print(f"      • option={opt1!r} → available={q}", flush=True)

                            else:

                                print(f"      ❌ Error updating inventory for option={opt1!r}: {inv_res.text}",
                                      flush=True)

                        # 2) Создание Article metafield

                        print(f"    ✅ СТВОРЕНО ({created}), ID={pid}", flush=True)

                        art = recs[0][idx["Articul"]].strip()

                        res = shopify_request(client, "POST",

                                              f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products/{pid}/metafields.json",

                                              json={"metafield": {

                                                  "namespace": "custom", "key": "Article",

                                                  "value": art, "type": "single_line_text_field"

                                              }})

                        if res.status_code < 300:

                            print(f"    ✨ Створено Article → '{art}'", flush=True)

                        else:

                            print(f"    ❌ Помилка створення Article: {res.text}", flush=True)

                        # 3) Остальные metafields

                        for mf_item in mf:

                            if mf_item["key"] == "Article":
                                continue

                            res = shopify_request(client, "POST",

                                                  f"https://{SHOP_NAME}.myshopify.com/admin/api/{API_VERSION}/products/{pid}/metafields.json",

                                                  json={"metafield": mf_item})

                            if res.status_code < 300:

                                print(f"    ✅ MF '{mf_item['key']}' створено", flush=True)

                            else:

                                print(f"    ❌ Помилка створення MF '{mf_item['key']}': {res.text}", flush=True)


                    else:


                        print(f"    ❌ Помилка створення товару: {r2.text}", flush=True)

            print(f"\n🏁 Синхронізація завершена: створено={created}, оновлено={updated}\n", flush=True)
            flash(f": Синхронізація завершена: створено={created}, оновлено={updated}")

            try:
                ftp = FTP(FTP_HOST)
                ftp.login(FTP_USER, FTP_PASS)

                # Определяем директорию и имя файла
                directory, filename = os.path.split(FTP_FILE_PATH)
                directory = directory or "/"

                # Список файлов в директории
                files = ftp.nlst(directory)
                print(f"📂 Вміст FTP-директорії «{directory}»: ", flush=True)
                for f in files:
                    print(f"    – {directory.rstrip('/')}/{f}", flush=True)

                # Удаляем наш файл
                if filename in [os.path.basename(f) for f in files]:
                    ftp.delete(FTP_FILE_PATH)
                    print(f"🗑️ Файл {FTP_FILE_PATH} видалено з FTP", flush=True)
                else:
                    print(f"⚠️ Файл {FTP_FILE_PATH} не знайдено — нічого не видаляємо", flush=True)

                ftp.quit()

            except Exception as e:
                print(f"❌ Не вдалося обробити FTP-директорію {directory}: {e}", flush=True)

            app.config["LAST_LOGS"] = buf_stdout.getvalue().splitlines()

            SYNC_IN_PROGRESS = False
            return redirect(url_for("report"))



if __name__ == '__main__':
    app.run(host='0.0.0.0', port=80, debug=True)


