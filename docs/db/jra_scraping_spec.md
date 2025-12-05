# JRA 過去レース結果（SKL/SRL/SDE）スクレイピング技術調査ドキュメント

**Version 1.0（2025-12 時点調査）**

---

## 🏗 全体構造（JRA 過去レース結果ページの階層）

```
過去レース結果トップ（SKL）
  └─ 開催日一覧（SRL）
       └─ レース一覧（SDE）
            └─ 各レース詳細（SDE→SDE/DETAIL）
```

- **SKL（Search Key List）** … 年月指定ページ
- **SRL（Search Race List）** … 開催日・競馬場別一覧
- **SDE（Search Detail Entry）** … 各レース情報

---

# 1. SKL（年月指定ページ）

## 1.1 DOM 構造：年・月セレクトボックス

```html
<select id="kaisaiY_list" name="kaisaiY_list" class="dropdown-select">
  <option value="2025">2025</option>
  <option value="2024">2024</option>
  ...
</select>

<select id="kaisaiM_list" name="kaisaiM_list" class="dropdown-select">
  <option value="01">1</option>
  <option value="02">2</option>
  ...
</select>
```

- どちらも **onchange="getSelectData()"** を持つ。

## 1.2 月送りボタン（Prev / Next）

```html
<input type="button" class="btn_prevMonth" value="＜ 前の月" onclick="changeDisplayMonth('-1')">
<input type="button" class="btn_nextMonth" value="次の月 ＞" onclick="changeDisplayMonth('1')">
```

---

# 2. SKL の JavaScript 解析

## 2.1 `getSelectData()`

```javascript
function getSelectData(){
  idx1 = document.getElementById("kaisaiY_list").selectedIndex;
  idx2 = document.getElementById("kaisaiM_list").selectedIndex;
  param1 = document.getElementById("kaisaiY_list").options[idx1].value;
  param2 = document.getElementById("kaisaiM_list").options[idx2].value;
  setParameter(param1, param2);
}
```

## 2.2 `changeDisplayMonth()`

```javascript
function changeDisplayMonth(arg){
  idx1 = document.getElementById("kaisaiY_list").selectedIndex;
  idx2 = document.getElementById("kaisaiM_list").selectedIndex;
  param1 = Number(document.getElementById("kaisaiY_list").options[idx1].value);
  param2 = Number(document.getElementById("kaisaiM_list").options[idx2].value);

  var d = new Date(param1,param2-1,1);
  d.setMonth(d.getMonth() + Number(arg));

  var newY = d.getFullYear();
  var newM = d.getMonth() + 1;
  if(newM < 10) newM = '0' + newM;

  setParameter(newY,newM);
}
```

---

# 3. CNAME の生成ロジック

## 3.1 `setParameter()`

```javascript
function setParameter(arg1, arg2){
  var yearMonth = "202512";
  arg = arg1 + arg2; // YYYYMM

  if(Number(arg) >= yearMonth){
    param = 'pw01skl00' + String(arg) + '/';
  } else {
    param = 'pw01skl10' + String(arg) + '/';
  }

  cname = param + objParam[String(arg).substring(2,6)];
  doAction('/JRADB/accessS.html',cname);
}
```

### CNAME 構造まとめ

```
>= 2025/12 → pw01skl00YYYYMM/XX
<  2025/12 → pw01skl10YYYYMM/XX
```

例：

```
2024年12月 → pw01skl10202412/AB
2025年01月 → pw01skl00202501/3F
```

---

# 4. doAction() の動作

```javascript
function doAction(url,cnameValue){
  document.getElementById("commForm01").action = url;
  document.getElementById("cname").value = cnameValue;
  document.getElementById("commForm01").submit();
}
```

### → `POST /JRADB/accessS.html` に `cname` を送信

---

# 5. SRL（開催日一覧）の構造

## 5.1 リンク例

```html
<a onclick="doAction('/JRADB/accessS.html','pw01srl10062023050120231202/B2')">
  中山 2023/12/02
</a>
```

抽出項目：

- `srl_cname`
- `date_yyyymmdd`
- `course`

---

# 6. SDE（レース一覧）の構造

```html
<a href="?CNAME=pw01sde1006202305010120231202/7D">
  2歳未勝利
</a>
```

抽出項目：

- race_number
- race_name
- sde_cname

---

# 7. Python 実装方針

## 7.1 CNAME 生成（Python）

```python
OBJ_PARAM = {
    "2501": "3F",
    "2502": "0D",
    # ... 全マッピング
}

BORDER = 202512

def make_skl_cname(year: int, month: int) -> str:
    arg = f"{year}{month:02d}"
    yy_mm = arg[2:]
    suffix = OBJ_PARAM[yy_mm]
    prefix = "pw01skl00" if int(arg) >= BORDER else "pw01skl10"
    return f"{prefix}{arg}/{suffix}"
```

## 7.2 SKL HTML の取得

```python
import requests

def fetch_skl_month_html(year, month):
    cname = make_skl_cname(year, month)
    resp = requests.post(
        "https://www.jra.go.jp/JRADB/accessS.html",
        data={"cname": cname}
    )
    resp.raise_for_status()
    return resp.text
```

## 7.3 SRL/SDE の抽出

```python
import re

srl = re.findall(r"pw01srl[0-9A-Za-z/]+", html)
sde = re.findall(r"pw01sde[0-9A-Za-z/]+", html)
```

---

# 8. DevTools で使う調査プロンプト

```text
現在のページから以下の情報を JSON で抽出してください：

1. 年月セレクトボックス（id/name/options）
2. onchange ハンドラ
3. 前月/次月ボタンの onclick
4. getSelectData, setParameter, changeDisplayMonth, doAction の関数ソース
5. objParam の内容
6. CNAME の生成ロジックの要約
```

---

# 9. 結論

- **SKL の CNAME は完全再現可能 → Selenium 不要**
- **SRL / SDE は HTML パースで抽出可能**
- 任意年月 → 開催日 → レース一覧 → レース詳細まで
  **完全自動スクレイピングルートが構築可能**

---

本ドキュメントを必要に応じて拡張し、ETL 実装仕様書にも転用可能です。

