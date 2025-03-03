---
displayed_sidebar: docs
---

# last_day

指定された日付部分に基づいて、入力された DATE または DATETIME 式の最後の日を返します。例えば、`last_day('2023-05-10', 'month')` は '2023-05-10' が属する月の最後の日を返します。

日付部分が指定されていない場合、この関数は指定された日付の月の最後の日を返します。

この関数は v3.1 からサポートされています。

## 構文

```SQL
DATE last_day(DATETIME|DATE date_expr[, VARCHAR unit])
```

## パラメータ

- `date_expr`: DATE または DATETIME 式、必須。

- `unit`: 日付部分、オプション。 有効な値には `month`、`quarter`、`year` が含まれ、デフォルトは `month` です。`unit` が無効な場合、エラーが返されます。

## 戻り値

DATE 値を返します。

## 例

```Plain
MySQL > select last_day('2023-05-10', 'month');
+----------------------------------+
| last_day('2023-05-10', 'month')  |
+----------------------------------+
| 2023-05-31                       |
+----------------------------------+

MySQL > select last_day('2023-05-10');
+------------------------+
| last_day('2023-05-10') |
+------------------------+
| 2023-05-31             |
+------------------------+

MySQL > select last_day('2023-05-10', 'quarter');
+-----------------------------------+
| last_day('2023-05-10', 'quarter') |
+-----------------------------------+
| 2023-06-30                        |
+-----------------------------------+

MySQL > select last_day('2023-05-10', 'year');
+---------------------------------------+
| last_day('2023-05-10', 'year')        |
+---------------------------------------+
| 2023-12-31                            |
+---------------------------------------+
```

## キーワード

LAST_DAY, LAST