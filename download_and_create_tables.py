# Databricks notebook source
# MAGIC %md
# MAGIC # SAPバイク販売データのダウンロードとテーブル作成
# MAGIC
# MAGIC このノートブックは、SAP Datasphere サンプルリポジトリ(https://github.com/SAP-samples/datasphere-content/tree/main/Sample_Bikes_Sales_content) からCSVファイルをダウンロードし、DatabricksでDeltaテーブルを作成します。
# MAGIC
# MAGIC 本ノートブックには、[Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0)で配布されている`Sample_Bikes_Sales_content`を含みます。
# MAGIC
# MAGIC 本ノートブックで行った主な変更点：
# MAGIC - データ形式を変換（csv -> deltalake）
# MAGIC - yyyyMMddの文字列型で格納されたデータを日付型に変更
# MAGIC - テーブル、カラムコメントを追記

# COMMAND ----------

import requests
import pandas as pd
from pyspark.sql.types import *
from pyspark.sql import functions as F
import os

# COMMAND ----------

# MAGIC %md
# MAGIC ## 設定

# COMMAND ----------

# DBTITLE 1,パラメータ設定
# Widgetsの作成
dbutils.widgets.text("catalog", "skato", "カタログ名")
dbutils.widgets.text("schema", "bikes_sales_content", "スキーマ名")

# Widgetからの値の取得
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# 設定値取得
current_user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()
token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

# カタログ・スキーマ・ボリュームのデフォルト設定
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
spark.sql(f"CREATE SCHEMA {schema}")
spark.sql(f"USE SCHEMA {schema}")
spark.sql(f"CREATE VOLUME volume")

# ストレージパス / テーブル名の設定
schema_path = f"/Volumes/{catalog}/{schema}/volume/schema"
checkpoint_path = f"/Volumes/{catalog}/{schema}/volume/checkpoint"
data_path = f"/Volumes/{catalog}/{schema}/volume/data"

# COMMAND ----------

# DBTITLE 1,ロード対象csvの設定
# Base URL for raw GitHub files
BASE_URL = "https://raw.githubusercontent.com/SAP-samples/datasphere-content/main/Sample_Bikes_Sales_content/CSV"

# CSV files to download
CSV_FILES = [
    "Addresses.csv",
    "BusinessPartners.csv", 
    "Employees.csv",
    "ProductCategories.csv",
    "ProductCategoryText.csv",
    "ProductTexts.csv",
    "Products.csv",
    "SalesOrderItems.csv",
    "SalesOrders.csv"
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## CSVファイルのダウンロードとテーブル作成

# COMMAND ----------

# DBTITLE 1,csvダウンロード・テーブル作成関数を定義
from pyspark.sql.functions import col, to_date, length

def download_csv_file(filename):
    """Download a CSV file from GitHub"""
    url = f"{BASE_URL}/{filename}"
    print(f"Downloading {filename} from {url}")
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        # Save to temporary location
        temp_path = f"{data_path}"
        with open(temp_path, 'wb') as f:
            f.write(response.content)
        
        print(f"Successfully downloaded {filename}")
        return temp_path
    except Exception as e:
        print(f"Error downloading {filename}: {str(e)}")
        return None

def create_table_from_csv(csv_path, table_name):
    """Create a Delta table from CSV file"""
    try:
        # Read CSV with Spark, explicitly set encoding to UTF-8 to avoid mojibake
        df = spark.read.option("header", "true").option("inferSchema", "true").option("encoding", "UTF-8").csv(csv_path)
                
        print(f"\nSample data for {table_name}:")
        display(df.limit(5))
        print(f"Schema for {table_name}:")
        df.printSchema()
        print(f"Row count: {df.count()}")
        
        df.write.mode("overwrite").saveAsTable(f"{table_name}")
        
        print(f"Successfully created table: {table_name}")
        return True
    except Exception as e:
        print(f"Error creating table {table_name}: {str(e)}")
        return False

# COMMAND ----------

# DBTITLE 1,すべてのファイルをダウンロードしてテーブルを作成
successful_tables = []
failed_tables = []

for csv_file in CSV_FILES:
    print(f"\n{'='*50}")
    print(f"Processing {csv_file}")
    print(f"{'='*50}")
    
    # Download CSV
    temp_path = download_csv_file(csv_file)
    
    if temp_path:
        # Create table name (remove .csv extension and add prefix)
        table_name = csv_file.replace('.csv', '').lower()
        
        # Create table
        if create_table_from_csv(temp_path, table_name):
            successful_tables.append(table_name)
        else:
            failed_tables.append(table_name)
        
        # # Clean up temp file
        # try:
        #     os.remove(temp_path)
        # except:
        #     pass
    else:
        failed_tables.append(csv_file.replace('.csv', ''))

# COMMAND ----------

# DBTITLE 1,取得結果を要約
print("\n" + "="*60)
print("PROCESSING SUMMARY")
print("="*60)

print(f"\nSuccessfully created {len(successful_tables)} tables:")
for table in successful_tables:
    print(f"  ✓ {table}")

if failed_tables:
    print(f"\nFailed to create {len(failed_tables)} tables:")
    for table in failed_tables:
        print(f"  ✗ {table}")

print(f"\nTotal tables in database:")
tables_df = spark.sql(f"SHOW TABLES IN {schema}")
tables_df.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 型変更、コメント、リレーション情報の付与

# COMMAND ----------

# DBTITLE 1,yyyyMMddで格納された文字列を日付型に変更
date_column_list = spark.sql(f"""
SELECT
  table_schema
  , table_name
  , column_name
FROM
  skato.information_schema.columns
WHERE
  table_schema = '{schema}'
  AND (column_name LIKE '%DAT' OR column_name LIKE '%DATE')
""")

for row in date_column_list.collect():
    table = f"{row['table_name']}"
    column = row['column_name']
    df = spark.read.table(table)
    if column in df.columns:
        df = df.withColumn(column, F.to_date(F.col(column).cast("string"), "yyyyMMdd"))
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

# COMMAND ----------

# DBTITLE 1,余分なカラムを除外
# MAGIC %sql
# MAGIC ALTER TABLE employees SET TBLPROPERTIES ('delta.columnMapping.mode' = 'name'); 
# MAGIC ALTER TABLE employees DROP COLUMNS (_c13, _c14, _c15, _c16, _c17, _c18);

# COMMAND ----------

# DBTITLE 1,テーブル説明・カラム説明を追記
# MAGIC %sql
# MAGIC -- salesorders テーブル
# MAGIC COMMENT ON TABLE salesorders IS "売上注文に関連するデータが含まれるテーブルで、注文管理の包括的なビューを提供します。売上パフォーマンスの追跡、顧客関係の管理、財務報告のコンプライアンスを確保するために使用できます。";
# MAGIC
# MAGIC -- addresses テーブル
# MAGIC COMMENT ON TABLE addresses IS "このテーブルは、住所の地理的および管理的属性を含む詳細な情報を提供します。テーブルは、位置のマッピングや住所の有効性の分析、住所の種別（住宅または商業など）による分類など、さまざまな目的に活用できます。都市、郵便番号、通り名、および地理座標などの主要データポイントが含まれており、ロジスティクス、配送計画、位置情報サービスの支援に役立ちます。";
# MAGIC
# MAGIC -- productcategories テーブル
# MAGIC COMMENT ON TABLE productcategories IS "システム内の製品カテゴリに関する情報が含まれるテーブルです。各製品カテゴリの識別子、レコード作成者、作成日時が記録されており、製品の分類追跡や変更の監査、エントリーの履歴理解に活用されます。";
# MAGIC
# MAGIC -- products テーブル
# MAGIC COMMENT ON TABLE products IS "在庫製品に関する詳細データを含むテーブル。製品の識別、カテゴリ、価格、寸法、サプライヤー情報を提供。在庫管理、販売分析、サプライヤー管理など多岐に渡る用途。データは製品変更の追跡や税制規則の遵守を支援。";
# MAGIC
# MAGIC -- businesspartners テーブル
# MAGIC COMMENT ON TABLE businesspartners IS "このテーブルには、組織に関連するパートナー情報が含まれており、パートナー関係の管理や貢献の分析、およびコミュニケーション支援に利用できます。さらに、パートナーレコードの作成と変更履歴を追跡し、データ管理と監査を支援します。";
# MAGIC
# MAGIC -- employees テーブル
# MAGIC COMMENT ON TABLE employees IS "社員情報を含むテーブルであり、従業員のデータを管理し、組織内でのコミュニケーションを促進するために使用されます。従業員の個人情報や雇用状況が含まれており、雇用履歴の追跡や正確な報告を支援します。";
# MAGIC
# MAGIC -- salesorderitems テーブル
# MAGIC COMMENT ON TABLE salesorderitems IS "販売注文とそれに関連するアイテムに関する詳細な情報が含まれています。注文とアイテムの一意の識別子、製品の詳細、金額（控除前後）、税情報、アイテムの状態などが記録されており、売上パフォーマンスの追跡や在庫管理、収益分析、財務規制への適合、物流および顧客コミュニケーションに役立ちます。";
# MAGIC
# MAGIC -- producttexts テーブル
# MAGIC COMMENT ON TABLE producttexts IS "製品に関する詳細情報を含む表で、製品のカタログ作成、マーケティング資料、顧客サポートに利用できます。";
# MAGIC
# MAGIC -- productcategorytext テーブル
# MAGIC COMMENT ON TABLE productcategorytext IS "このテーブルは、製品カテゴリに関する情報を含んでおり、製品を効果的にカテゴリ分けし、各カテゴリに関する素早く詳細な情報をユーザーに提供するために使用されます。製品検索機能の強化、ユーザーエクスペリエンスの向上、製品カテゴリの理解を深めたマーケティング活動のサポートなど、さまざまな用途が考えられます。";
# MAGIC
# MAGIC -- addresses カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.ADDRESSID IS '各アドレスエントリの固有の識別子で、アドレスレコードの簡単な参照と管理を可能にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.CITY IS '住所に関連付けられた都市の名前で、地理的なコンテキストを提供する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.POSTALCODE IS '郵便物の配達や場所の特定に不可欠な住所の郵便番号。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.STREET IS '住所が存在する通りの名前で、正確な場所を特定するのに役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.BUILDING IS '住所に存在する特定の建物を識別するための識別子で、同じ通り上にある複数の建物を区別するのに役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.COUNTRY IS '住所が存在する国の名前、国際的な文脈では重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.REGION IS '住所に関連付けられた地域または州、追加の地理的詳細を提供する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.ADDRESSTYPE IS '住所の種類を表すコードで、住宅、商業などであるかどうかを示すことができる。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.VALIDITY_STARTDATE IS 'アドレスが有効とみなされる日付。時間の経過に伴う変更を追跡するのに役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.VALIDITY_ENDDATE IS '有効なアドレスを管理するために、アドレスが有効な期間を示す日付。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.LATITUDE IS '住所の緯度座標で、正確な地理的マッピングを可能にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.addresses.LONGITUDE IS '住所の緯度座標で、緯度と組み合わせて正確な位置を特定する。';
# MAGIC
# MAGIC -- businesspartners カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.PARTNERID IS 'システム内の各パートナーに一意の識別子を表し、パートナー関連データの簡単な参照と管理を可能にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.PARTNERROLE IS 'パートナーが組織内で果たす特定の役割または機能を示し、それにより彼らの責任や貢献を理解するのに役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.EMAILADDRESS IS 'パートナーに関連付けられた電子メールアドレスを含み、通信や連絡の目的で役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.PHONENUMBER IS 'パートナーの主な電話番号を保持し、必要に応じて直接連絡できるようにします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.FAXNUMBER IS 'パートナーのファックス番号を格納します。これは、特定の種類の文書化や通信に役立つ場合があります。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.WEBADDRESS IS 'パートナーのウェブサイトのURLを提供し、追加情報とオンラインプレゼンスのためのリソースを提供します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.ADDRESSID IS 'パートナーに関連付けられたアドレスを一意に識別するために使用され、他のアドレス関連データにリンクできます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.COMPANYNAME IS 'パートナーの会社の公式名称を表し、識別およびブランド化の目的で不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.LEGALFORM IS 'パートナーの組織の法的構造を記述し、契約および規制上の考慮事項に影響を及ぼす可能性があります。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.CREATEDBY IS 'パートナーレコードを作成したユーザーの識別子を示します。責任の追跡とデータ管理に役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.CREATEDAT IS 'パートナーレコードが作成されたときのタイムスタンプを記録し、データの履歴のコンテキストを提供します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.CHANGEDBY IS 'パートナーレコードを最後に変更したユーザーの識別子を表示し、明確な監査証跡を保持するのに役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.CHANGEDAT IS 'パートナーレコードに最後に加えられた変更のタイムスタンプを取得します。これは、データの更新を理解する上で重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.businesspartners.CURRENCY IS 'パートナーとの取引または取引で使用される通貨を指定します。これは、財務操作に不可欠です。';
# MAGIC
# MAGIC -- employees カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.EMPLOYEEID IS '各従業員に割り当てられた一意の識別子で、システム内での記録を追跡するために使用できます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.NAME_FIRST IS '従業員の名前、識別に個人的な感覚を提供する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.NAME_MIDDLE IS '従業員の中間名で、正式な身分証明または文書化に使用される場合があります。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.NAME_LAST IS '従業員の姓は、同様の名前を持つ個人の区別に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.NAME_INITIALS IS '従業員の名前の頭文字で、簡単な参照やスペースが限られている状況で使用できます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.SEX IS '従業員の性別で、人口統計学的分析や報告書の目的で関連する場合があります。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.LANGUAGE IS '従業員が主に使用する言語で、多様な労働力の中でのコミュニケーションとサポートに役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.PHONENUMBER IS '必要に応じて直接通信できるようにするための従業員の連絡先番号。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.EMAILADDRESS IS '従業員の公式の電子メールアドレスであり、電子通信および文書交換に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.LOGINNAME IS '社員が会社のシステムにアクセスするために使用するユーザー名で、セキュアでパーソナライズされたアクセスを保証する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.ADDRESSID IS '従業員の住所を識別するための識別子で、別の場所に保存されたより詳細な住所情報にリンクしています。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.VALIDITY_STARTDATE IS '従業員の記録が有効になる日付であり、雇用または状態の開始を示します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.employees.VALIDITY_ENDDATE IS '従業員の記録が有効でなくなった日付で、従業員の雇用またはステータスの終了を示します。';
# MAGIC
# MAGIC -- productcategories カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.productcategories.PRODCATEGORYID IS '各製品カテゴリの固有の識別子を表し、システム内での製品の簡単なカテゴリ化と取得を可能にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.productcategories.CREATEDBY IS 'レコードを作成したユーザーまたはシステムの識別子を示し、変更の追跡や責任の追及に役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.productcategories.CREATEDAT IS 'レコードが作成されたときのタイムスタンプを保存し、データの年齢と関連性のコンテキストを提供する。';
# MAGIC
# MAGIC -- productcategorytext カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.productcategorytext.PRODCATEGORYID IS '各製品カテゴリの固有の識別子を表し、関連製品の簡単なカテゴリ化と取得を可能にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.productcategorytext.LANGUAGE IS '製品の説明が提供される言語を示し、ユーザーが優先する言語で情報にアクセスできるようにします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.productcategorytext.SHORT_DESCR IS '製品カテゴリの簡単な説明を含み、ユーザーがカテゴリを一目で理解できるようにクイックオーバービューを提供します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.productcategorytext.MEDIUM_DESCR IS '製品カテゴリのより詳細な説明を提供し、ユーザーに短い説明を超えた追加のコンテキストと情報を提供します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.productcategorytext.LONG_DESCR IS '製品カテゴリの詳細な説明を提供し、機能、利点、その他の関連情報を含めて、ユーザーが情報に基づいた決定を下すのを支援します。';
# MAGIC
# MAGIC -- products カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.PRODUCTID IS '在庫にあるユニークな製品を識別し、製品の詳細を追跡および管理できるようにします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.TYPECODE IS '製品の分類を表し、レポートや分析のために製品をカテゴリ化するために使用できます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.PRODCATEGORYID IS '製品をその特定のカテゴリにリンクし、同様の製品の組織化と取得を容易にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.CREATEDBY IS '製品エントリを作成したユーザーの識別子を示します。監査と責任追跡に役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.CREATEDAT IS '製品が作成されたときのタイムスタンプを記録し、製品ライフサイクル管理のコンテキストを提供する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.CHANGEDBY IS '製品エントリを最後に変更したユーザーの識別子を表示します。変更や更新を追跡するために重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.CHANGEDAT IS '製品への最後の変更のタイムスタンプを取得し、バージョン管理と履歴追跡を支援します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.SUPPLIER_PARTNERID IS '製品に関連するサプライヤーを特定し、調達およびサプライヤー管理に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.TAXTARIFFCODE IS '製品の税分類を表し、税制上の規制に従うために必要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.QUANTITYUNIT IS '製品の数量の測定単位を指定し、在庫および販売取引の明確性を確保します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.WEIGHTMEASURE IS '出荷、取り扱い、在庫管理において重要となる製品の重量を示します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.WEIGHTUNIT IS '製品の重量の測定単位を定義し、重量報告の的一貫性を提供する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.CURRENCY IS '製品価格が記載されている通貨を指定し、財務取引や報告書の作成に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.PRICE IS '製品の販売価格を表し、販売分析および収益追跡に重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.WIDTH IS '製品の幅の寸法を示します。保管、出荷、展示の考慮に重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.DEPTH IS '製品のサイズやフィットを理解するために必要な製品の深さの次元を表します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.HEIGHT IS '製品の高さの寸法を把握し、空間計画および在庫管理を支援する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.DIMENSIONUNIT IS '製品の寸法の測定単位を指定し、サイズの報告における一貫性を確保します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.products.PRODUCTPICURL IS '製品画像へのURLリンクを含み、オンラインリストやマーケティング資料に不可欠です。';
# MAGIC
# MAGIC -- producttexts カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.producttexts.PRODUCTID IS 'データベース内のユニークな製品を識別し、製品情報の簡単な参照と取得を可能にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.producttexts.LANGUAGE IS '製品の説明が提供される言語を指定し、ユーザーが優先する言語で情報にアクセスできるようにします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.producttexts.SHORT_DESCR IS '製品の簡潔な概要を提供し、重要な機能と利点を強調しています。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.producttexts.MEDIUM_DESCR IS '製品の詳細な説明を提供し、その機能や使用方法について詳しく説明して、より深い理解を促進します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.producttexts.LONG_DESCR IS '製品の詳細な説明を含み、技術仕様、使用方法、そして深い知識のための他の関連情報が含まれています。';
# MAGIC
# MAGIC -- salesorderitems カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.SALESORDERID IS '各販売注文の一意の識別子を表し、特定の注文を追跡および参照することを可能にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.SALESORDERITEM IS '販売注文内の個々のアイテムを識別し、詳細な注文管理を容易にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.PRODUCTID IS '各製品の固有の識別子を含み、在庫管理と販売追跡に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.NOTEID IS '販売注文アイテムに関連するメモの識別子を保持します。追加のコンテキストまたは指示に役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.CURRENCY IS '取引が行われる通貨を指定し、財務報告および分析のために重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.GROSSAMOUNT IS '一切控除前の合計額を表し、初期取引価値の明確な概要を提供する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.NETAMOUNT IS '割引や返品などの控除後の最終額を示し、実際の売上からの収益を反映します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.TAXAMOUNT IS '販売注文アイテムに適用される合計税額を表示し、コンプライアンスと財務計算に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.ITEMATPSTATUS IS '販売注文のアイテムのステータスを説明し、在庫状況または処理ステージを示すことができる。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.OPITEMPOS IS 'アイテムの運用位置に関する情報を含み、物流または履行に関連する場合があります。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.QUANTITY IS 'アイテムの注文単位数を示し、在庫管理と注文履行に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.QUANTITYUNIT IS '数量の測定単位を指定し、注文の詳細を明確にする。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorderitems.DELIVERYDATE IS '販売注文アイテムの出荷予定日を表し、スケジューリングと顧客とのコミュニケーションに重要です。';
# MAGIC
# MAGIC -- salesorders カラム
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.SALESORDERID IS '各販売注文の一意の識別子を表し、簡単に追跡および参照できるようにします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.CREATEDBY IS '販売注文を作成したユーザーの識別子を示します。監査と責任追跡に役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.CREATEDAT IS '販売注文が作成されたときのタイムスタンプを記録し、注文の履歴に関するコンテキストを提供する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.CHANGEDBY IS '販売注文を最後に変更したユーザーの識別子を表示します。これは、変更を追跡するために重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.CHANGEDAT IS '販売注文に最後に加えられた変更のタイムスタンプを取得し、注文のライフサイクルを理解するのに役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.FISCVARIANT IS '販売注文に適用される財務バリアントを表し、財務報告および分析に影響を与えることができる。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.FISCALYEARPERIOD IS '販売注文が属する会計年度と期間を示し、財務計画と報告書のために不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.NOTEID IS '販売注文に関連するメモの識別子を含み、追加のコンテキストまたは指示を可能にします。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.PARTNERID IS '販売注文に関連するビジネスパートナーの識別子を表し、関係管理に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.SALESORG IS '販売組織構造とレポートのために重要な、販売注文を担当する販売組織を特定する。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.CURRENCY IS '販売注文を処理する通貨を指定します。財務取引や換算に関係します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.GROSSAMOUNT IS '一切の控除前の販売注文の合計額を反映し、注文の価値を明確に示します。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.NETAMOUNT IS '割引などの控除後の販売注文額を表し、収益計算に不可欠です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.TAXAMOUNT IS '販売注文に適用される合計税額を示し、コンプライアンスと財務報告のために重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.LIFECYCLESTATUS IS '販売注文のライフサイクルにおける現在のステータスを説明し、注文の進捗状況を管理および追跡するのに役立ちます。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.BILLINGSTATUS IS '販売注文の請求状況を表示し、財務の追跡と顧客とのコミュニケーションに重要です。';
# MAGIC COMMENT ON COLUMN bikes_sales_content.salesorders.DELIVERYSTATUS IS '販売注文の出荷状況を示します。物流と顧客満足度の両方に不可欠です。';

# COMMAND ----------

# DBTITLE 1,主キー制約を追加
# MAGIC %sql
# MAGIC -- BusinessPartner
# MAGIC ALTER TABLE businesspartners ALTER COLUMN PARTNERID SET NOT NULL;
# MAGIC ALTER TABLE businesspartners ADD PRIMARY KEY (PARTNERID) RELY;
# MAGIC
# MAGIC -- SalesOrders
# MAGIC ALTER TABLE salesorders ALTER COLUMN SALESORDERID SET NOT NULL;
# MAGIC ALTER TABLE salesorders ADD PRIMARY KEY (SALESORDERID) RELY;
# MAGIC
# MAGIC -- Products
# MAGIC ALTER TABLE products ALTER COLUMN PRODUCTID SET NOT NULL;
# MAGIC ALTER TABLE products ADD PRIMARY KEY (PRODUCTID) RELY;
# MAGIC
# MAGIC -- ProductCategories
# MAGIC ALTER TABLE productcategories ALTER COLUMN PRODCATEGORYID SET NOT NULL;
# MAGIC ALTER TABLE productcategories ADD PRIMARY KEY (PRODCATEGORYID) RELY;
# MAGIC
# MAGIC -- Employees
# MAGIC ALTER TABLE employees ALTER COLUMN EMPLOYEEID SET NOT NULL;
# MAGIC ALTER TABLE employees ADD PRIMARY KEY (EMPLOYEEID) RELY;
# MAGIC
# MAGIC -- Address
# MAGIC ALTER TABLE addresses ALTER COLUMN ADDRESSID SET NOT NULL;
# MAGIC ALTER TABLE addresses ADD PRIMARY KEY (ADDRESSID) RELY;

# COMMAND ----------

# DBTITLE 1,外部キー制約を追加
# MAGIC %sql
# MAGIC -- businesspartners → addresses
# MAGIC ALTER TABLE businesspartners ADD CONSTRAINT fk_bp_address
# MAGIC     FOREIGN KEY (ADDRESSID) REFERENCES addresses(ADDRESSID)
# MAGIC     RELY;
# MAGIC
# MAGIC -- salesorders → businesspartners
# MAGIC ALTER TABLE salesorders ADD CONSTRAINT fk_so_partner
# MAGIC     FOREIGN KEY (PARTNERID) REFERENCES businesspartners(PARTNERID)
# MAGIC     RELY;
# MAGIC
# MAGIC -- salesorders → employees（CreatedBy, ChangedByは社員IDと推測）
# MAGIC ALTER TABLE salesorders ADD CONSTRAINT fk_so_createdby
# MAGIC     FOREIGN KEY (CREATEDBY) REFERENCES employees(EMPLOYEEID)
# MAGIC     RELY;
# MAGIC ALTER TABLE salesorders ADD CONSTRAINT fk_so_changedby
# MAGIC     FOREIGN KEY (CHANGEDBY) REFERENCES employees(EMPLOYEEID)
# MAGIC     RELY;
# MAGIC
# MAGIC -- salesorderitems → salesorders
# MAGIC ALTER TABLE salesorderitems ADD CONSTRAINT fk_soi_salesorder
# MAGIC     FOREIGN KEY (SALESORDERID) REFERENCES salesorders(SALESORDERID)
# MAGIC     RELY;
# MAGIC
# MAGIC -- salesorderitems → products
# MAGIC ALTER TABLE salesorderitems ADD CONSTRAINT fk_soi_product
# MAGIC     FOREIGN KEY (PRODUCTID) REFERENCES products(PRODUCTID)
# MAGIC     RELY;
# MAGIC
# MAGIC -- products → productcategories
# MAGIC ALTER TABLE products ADD CONSTRAINT fk_products_category
# MAGIC     FOREIGN KEY (PRODCATEGORYID) REFERENCES productcategories(PRODCATEGORYID)
# MAGIC     RELY;
# MAGIC
# MAGIC -- employees → addresses
# MAGIC ALTER TABLE employees ADD CONSTRAINT fk_emp_address
# MAGIC     FOREIGN KEY (ADDRESSID) REFERENCES addresses(ADDRESSID)
# MAGIC     RELY;
# MAGIC
# MAGIC -- products → producttexts
# MAGIC ALTER TABLE producttexts ADD CONSTRAINT fk_products_text
# MAGIC     FOREIGN KEY (PRODUCTID) REFERENCES products(PRODUCTID)
# MAGIC     RELY;
# MAGIC
# MAGIC -- productcategories → productcategorytext
# MAGIC ALTER TABLE productcategorytext ADD CONSTRAINT fk_products_category_texts
# MAGIC     FOREIGN KEY (PRODCATEGORYID) REFERENCES productcategories(PRODCATEGORYID)
# MAGIC     RELY;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix. リレーション情報の可視化

# COMMAND ----------

def mermeaid_display(code):
    mermeid_html =f"""
                    <html>
                        <head>
                            <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
                            <script>mermaid.initialize({{startOnLoad:true}});</script>
                        </head>
                        <body>
                            <div class="mermaid">
                                {code}
                            </div>
                        </body>
                    </html>
                    """
    displayHTML(mermeid_html)

code = """
erDiagram

ADDRESSES {
  type ADDRESSID PK
}

BUSINESSPARTNERS {
  type PARTNERID PK
  type ADDRESSID FK
}

SALESORDERS {
  type SALESORDERID PK
  type PARTNERID FK
  type CREATEDBY FK
  type CHANGEDBY FK
}

EMPLOYEES {
  type EMPLOYEEID PK
  type ADDRESSID FK
}

SALESORDERITEMS {
  type SALESORDERITEMID PK
  type SALESORDERID FK
  type PRODUCTID FK
}

PRODUCTS {
  type PRODUCTID PK
  type PRODCATEGORYID FK
}

PRODUCTCATEGORIES {
  type PRODCATEGORYID PK
}

PRODUCTTEXTS {
  type PRODUCTID FK
}

PRODUCTCATEGORYTEXT {
  type PRODCATEGORYID FK
}

BUSINESSPARTNERS }|..|| ADDRESSES : "belongs to"
EMPLOYEES }|..|| ADDRESSES : "belongs to"
SALESORDERS }|..|| BUSINESSPARTNERS : "ordered by"
SALESORDERS }|..|| EMPLOYEES : "created by"
SALESORDERS }|..|| EMPLOYEES : "changed by"
SALESORDERITEMS }|..|| SALESORDERS : "in"
SALESORDERITEMS }|..|| PRODUCTS : "for"
PRODUCTS }|..|| PRODUCTCATEGORIES : "categorized as"
PRODUCTS ||--o{ PRODUCTTEXTS : "has"
PRODUCTCATEGORIES ||--o{ PRODUCTCATEGORYTEXT : "has"
"""

# COMMAND ----------

mermeaid_display(code)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC <html>
# MAGIC     <head>
# MAGIC         <link rel="stylesheet" href="https://unpkg.com/mermaid/dist/mermaid.min.css">
# MAGIC         <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
# MAGIC         <script>mermaid.initialize({startOnLoad:true});</script>
# MAGIC     </head>
# MAGIC     <body>
# MAGIC         <div class="mermaid">
# MAGIC
# MAGIC         </div>
# MAGIC     </body>
# MAGIC </html>

# COMMAND ----------

# MAGIC %md
# MAGIC ## Appendix. DDL生成（準備用）

# COMMAND ----------

# schema = 'bikes_sales_content'

# df = spark.sql(f"""
# SELECT
#  table_catalog
#  , table_schema
#  , table_name
#  , comment
#  , CONCAT('COMMENT ON TABLE ', table_name, ' IS "', comment, '";') as ddl
# FROM
#   skato.information_schema.tables
# WHERE
#   table_schema = '{schema}'
#           """)

# # display(df)
# for row in df.select("ddl").collect():
#     print(row["ddl"])

# COMMAND ----------

# df = spark.sql(f"""
# SELECT
#   table_catalog
#   , table_schema
#   , table_name
#   , column_name
#   , comment
#   , ai_translate(comment, "ja") as ja_comment
#   , CONCAT('COMMENT ON COLUMN ', table_schema, '.', table_name, '.', column_name, ' IS `', ja_comment, '`;') as ddl
# FROM
#   skato.information_schema.columns
# WHERE
#   table_schema = '{schema}'
#           """)

# for row in df.select("ddl").collect():
#     print(row["ddl"])
