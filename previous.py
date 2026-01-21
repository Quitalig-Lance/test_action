import pandas as pd
import requests
import os
import logging
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load local .env for your manual tests
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DB_URL = os.getenv('DB_URL', '')
GBIZ_TOKEN = os.getenv('GBIZ_API_KEY', 'hUSgZr1FiAcDqvZA9UN9ZUFSXhkkNBMU')
BASE_URL = "https://info.gbiz.go.jp/hojin/v1/hojin/updateInfo"

# Mapping endpoints to table names
ENDPOINTS_MAP = {
    "": "corporate_basic_information_gbizinfo",
    "/certification": "notification_certification_information_gbizinfo",
    "/commendation": "award_information_gbizinfo",
    "/finance": "financial_information_gbizinfo",
    "/patent": "patent_information_gbizinfo",
    "/procurement": "procurement_information_gbizinfo",
    "/subsidy": "subsidy_information_gbizinfo",
    "/workplace": "workplace_information_gbizinfo"
}

TABLE_CONFIG = {
    "corporate_basic_information_gbizinfo": {
        "record_path": None,
        "meta": []
    },
    "notification_certification_information_gbizinfo": {
        "record_path": [ "certification" ],
        "meta": [ "corporate_number", "name", "location" ]
    },
    "award_information_gbizinfo": {
        "record_path": [ "commendation" ],
        "meta": [ "corporate_number", "name", "location" ]
    },
    "financial_information_gbizinfo": {
        "record_path": [ "finance", "management_index" ],
        "meta": [
            "corporate_number", "name", "location",
            ["finance", "accounting_standards"],
            ["finance", "fiscal_year_cover_page"],
            ["finance", "sh1_n"], ["finance", "sh1_r"],
            ["finance", "sh2_n"], ["finance", "sh2_r"],
            ["finance", "sh3_n"], ["finance", "sh3_r"],
            ["finance", "sh4_n"], ["finance", "sh4_r"],
            ["finance", "sh5_n"], ["finance", "sh5_r"]
        ]
    },
    "patent_information_gbizinfo": {
        "record_path": [ "patent" ],
        "meta": [ "corporate_number", "name", "location" ]
    },
    "procurement_information_gbizinfo": {
        "record_path": [ "procurement" ],
        "meta": [ "corporate_number", "name", "location" ]
    },
    "subsidy_information_gbizinfo": {
        "record_path": [ "subsidy" ],
        "meta": [ "corporate_number", "name", "location" ]
    },
    "workplace_information_gbizinfo": {
        "record_path": None,
        "meta": [ "corporate_number", "name", "location" ]
    }
}

# --- COLUMN MAPPING CONFIGURATION ---
# Format: 'API_JSON_KEY': 'DB_COLUMN_NAME'
MAPPING_CONFIG = {
    "corporate_basic_information_gbizinfo": {
        "corporate_number": "corporate_number",
        "name": "corporate_name",
        "kana": "corporate_furigana",
        "name_en": "corporate_en_name",
        "postal_code": "postal_code",
        "location": "headquarters_address",
        "status": "status",
        "close_date": "close_date",
        "close_cause": "close_cause",
        "representative_name": "corporate_representative_name",
        "representative_position": "corporate_representative_position",
        "capital_stock": "capital",
        "employee_number": "employees",
        "company_size_male": "employees_male",
        "company_size_female": "employees_female",
        "business_items": "product_lineup_list",
        "business_summary": "business_overview",
        # company_website
        "date_of_establishment": "date_of_establishment",
        # year_of_founding,
        "update_date": "last_updated_date",
        "qualification_grade": "qualification_level"
    },
    "notification_certification_information_gbizinfo": {
        "corporate_number": "corporate_number",
        ######### TO REMOVE #########
        "name": ["name_corporate_number", "name"],
        "location": ["headquarters_addrss_corporate_number", "headquarters_address"],
        ######### TO REMOVE #########
        "date_of_approval": "certification_date",
        "title": "notification_certification",
        "target": "target",
        "category": "department",
        # company_size,
        "expiration_date": "expiration_date",
        "government_departments": "ministry_agency"
    },
    "award_information_gbizinfo": {
        "corporate_number": "corporate_number",
        ######### TO REMOVE #########
        "name": ["corporate_name", "award_corporate_name"],
        "location": ["headquarters_address", "award_headquarters_address"],
        ######### TO REMOVE #########
        "date_of_commendation": "certification_date",
        "title": "award_name",
        "target": "award_target",
        "category": "department",
        "government_departments": "ministry_agency"
    },
    "financial_information_gbizinfo": {
        "corporate_number": "corporate_number",
        ######### TO REMOVE #########
        "name": ["corporate_name", "financial_corporate_name"],
        "location": ["headquarters_address", "financial_headquarters_address"],
        ######### TO REMOVE #########
        "finance.accounting_standards": "accounting_standard",
        "finance.fiscal_year_cover_page": "fiscal_year",
        
        "period": "fiscal_period",
        "net_sales_summary_of_business_results": "net_sales",
        "net_sales_summary_of_business_results_unit_ref": "net_sales_unit",
        "operating_revenue1_summary_of_business_results": "operating_revenue",
        "operating_revenue1_summary_of_business_results_unit_ref": "operating_revenue_unit",
        # "operating_income",
        # "operating_income_unit",
        "gross_operating_revenue_summary_of_business_results": "total_operating_revenue",
        "gross_operating_revenue_summary_of_business_results_unit_ref": "total_operating_revenue_unit",
        "ordinary_income_summary_of_business_results": "ordinary_revenue",
        "ordinary_income_summary_of_business_results_unit_ref": "ordinary_revenue_unit",
        "net_premiums_written_summary_of_business_results_ins": "net_premiums_written",
        "net_premiums_written_summary_of_business_results_ins_unit_ref": "net_premiums_written_unit",
        "ordinary_income_loss_summary_of_business_results": "ordinary_profit_or_loss",
        "ordinary_income_loss_summary_of_business_results_unit_ref": "ordinary_profit_or_loss_unit",
        "net_income_loss_summary_of_business_results": "net_income_or_loss",
        "net_income_loss_summary_of_business_results_unit_ref": "net_income_or_loss_unit",
        "capital_stock_summary_of_business_results": "capital_stock",
        "capital_stock_summary_of_business_results_unit_ref": "capital_stock_unit",
        "net_assets_summary_of_business_results": "net_assets",
        "net_assets_summary_of_business_results_unit_ref": "net_assets_unit",
        "total_assets_summary_of_business_results": "total_assets",
        "total_assets_summary_of_business_results_unit_ref": "total_assets_unit",
        "number_of_employees": "number_of_employees",
        "number_of_employees_unit_ref": "number_of_employees_unit",
        "finance.sh1_n": "major_shareholder1", "finance.sh1_r": "major_shareholder1_ratio",
        "finance.sh2_n": "major_shareholder2", "finance.sh2_r": "major_shareholder2_ratio",
        "finance.sh3_n": "major_shareholder3", "finance.sh3_r": "major_shareholder3_ratio",
        "finance.sh4_n": "major_shareholder4", "finance.sh4_r": "major_shareholder4_ratio",
        "finance.sh5_n": "major_shareholder5", "finance.sh5_r": "major_shareholder5_ratio"
    },
    "patent_information_gbizinfo": {
        "corporate_number": "corporate_number",
        "name": ["corporate_name", "patent_corporate_name"],
        "location": ["headquarters_address", "patent_headquarters_address"],
        "patent_type": "patent_design_trademark",
        "application_number": "application_number",
        "application_date": "application_date",
        
        "fi_code": "patent_fi_code",
        "fi_jp": "patent_fi_code_jp",
        "fterm_code": "patent_f_term_theme_code",
        
        "design_code": "design_new_classification_code",
        "design_jp": "design_new_classification_code_jp",
        
        "trademark_code": "trademark_class_code",
        "trademark_jp": "trademark_class_code_jp",
        "title": "invention_name_or_design_or_trademark"
    },
    "procurement_information_gbizinfo": {
        "corporate_number": "corporate_number",
        "name": ["corporate_name", "procurement_name"],
        "location": ["headquarters_address", "procurement_headquarters_address"],
        "date_of_order": "order_date",
        "title": "project_name",
        "amount": "amount",
        "government_departments": "ministry_agency",
        "joint_signatures": "joint_name_list"
    },
    "subsidy_information_gbizinfo": {
        "corporate_number": "corporate_number",
        "name": ["corporate_name", "subsidy_corporate_name"],
        "location": ["headquarters_address", "subsidy_headquarters_address"],
        "date_of_approval": "certification_date",
        "title": "subsidy",
        "amount": "amount",
        "target": "target",
        "government_departments": "ministry_agency",
        "note": "remarks",
        "joint_signatures": "joint_name_list",
        "subsidy_resource": "subsidy_source"
    },
    "workplace_information_gbizinfo": {
        "corporate_number": "corporate_number",
        "name": ["corporate_name", "workplace_corporate_name"],
        "location": ["headquarters_address", "workplace_headquarters_address"],
        "workplace_info.base_infos.average_continuous_service_years_type": "average_years_of_service_range",
        "workplace_info.base_infos.average_continuous_service_years_Male": "average_years_of_service_male",
        "workplace_info.base_infos.average_continuous_service_years_Female": "average_years_of_service_female",
        "workplace_info.base_infos.average_continuous_service_years": "average_years_of_service_for_permanent_employees",
        "workplace_info.base_infos.average_age": "average_age_of_employees",
        "workplace_info.base_infos.month_average_predetermined_overtime_hours": "average_monthly_overtime_hours",
        
        "workplace_info.women_activity_infos.female_workers_proportion_type": "ratio_of_female_employees_range",
        "workplace_info.women_activity_infos.female_workers_proportion": "ratio_of_female_employees",
        "workplace_info.women_activity_infos.female_share_of_manager": "number_of_female_managers",
        "workplace_info.women_activity_infos.gender_total_of_manager": "total_number_of_managers_male_and_female",
        "workplace_info.women_activity_infos.female_share_of_officers": "number_of_female_executives",
        "workplace_info.women_activity_infos.gender_total_of_officers": "total_number_of_executives_male_and_female",
        
        "workplace_info.compatibility_of_childcare_and_work.number_of_paternity_leave": "eligible_for_childcare_leave_male",
        "workplace_info.compatibility_of_childcare_and_work.number_of_maternity_leave": "eligible_for_childcare_leave_female",
        "workplace_info.compatibility_of_childcare_and_work.paternity_leave_acquisition_num": "taking_childcare_leave_male",
        "workplace_info.compatibility_of_childcare_and_work.maternity_leave_acquisition_num": "taking_childcare_leave_female"
    }
}

def prepare_financial_records(records):
    """Sorts shareholders and injects top 5 into the finance object for flattening."""
    for record in records:
        finance = record.get("finance", {})
        if not finance: continue
        
        sh_list = finance.get("major_shareholders", [])
        if isinstance(sh_list, list):
            # Sort by ratio descending
            sorted_sh = sorted(sh_list, key=lambda x: float(x.get("shareholding_ratio") or 0), reverse=True)
            
            # Inject top 5 into the finance dictionary with simple keys for meta-tracking
            for i in range(1, 6):
                if i <= len(sorted_sh):
                    finance[f"sh{i}_n"] = sorted_sh[i-1].get("name_major_shareholders")
                    finance[f"sh{i}_r"] = sorted_sh[i-1].get("shareholding_ratio")
                else:
                    finance[f"sh{i}_n"] = None
                    finance[f"sh{i}_r"] = None

    return records

def sync_endpoint(engine, endpoint_suffix, table_name, from_date, to_date):
    headers = {'accept': 'application/json', 'X-hojinInfo-api-token': GBIZ_TOKEN}
    page = 1
    
    print(f"\n>>> Syncing {table_name}...")

    while True:
        params = {'page': page, 'from': from_date, 'to': to_date}
        response = requests.get(f"{BASE_URL}{endpoint_suffix}", headers=headers, params=params)
        
        if response.status_code != 200:
            logging.error(f"Error {response.status_code} on {table_name}")
            break
            
        data = response.json()
        records = data.get("hojin-infos") or data.get("update_infos")
        
        if not records:
            print(f"    - No new records found for {table_name}.")
            break

        nested_keys = {
        "notification_certification_information_gbizinfo": "certification",
        "award_information_gbizinfo": "commendation",
        "financial_information_gbizinfo": "finance",
        
        "subsidy_information_gbizinfo": "subsidy",
        # ... add others like 'patent', 'finance'
    }
    
    list_key = nested_keys.get(table_name)

    # 2. Flatten the data
    if list_key:
        # This creates a new row for EVERY item in the 'certification' list
        # record_path: the path to the list
        # meta: fields from the parent level to keep in every row
        df = pd.json_normalize(
            records,
            record_path=[list_key],
            meta=['corporate_number', 'update_date'],
            errors='ignore'
        )
    else:
        df = pd.DataFrame(records)

    # 3. Apply your Column Mapping
    column_map = MAPPING_CONFIG.get(table_name, {})
    df = df.rename(columns=column_map)

    if table_name == "notification_certification_information_gbizinfo":
        if 'approval_date' in df.columns:
            # This removes any rows where the date is None, NaN, or empty string
            initial_count = len(df)
            df = df.dropna(subset=['approval_date'])
            # Optional: Also filter out empty strings if the API sends "" instead of null
            df = df[df['approval_date'] != ""]
            
            print(f"    - Filtered out {initial_count - len(df)} records with null approval dates.")
    
    if not df.empty:
        # Filter to only DB columns and export to SQL
        db_cols = list(column_map.values())
        df = df[[c for c in db_cols if c in df.columns]]
        
        df.to_sql('temp_upsert', engine, if_exists='replace', index=False)
    
    with engine.begin() as conn:
        cols_str = ", ".join([f'"{c}"' for c in df.columns])
        # Update your SQL logic here to match your unique constraints
        query = f"""
            INSERT INTO {table_name} ({cols_str})
            SELECT {cols_str} FROM temp_upsert
            ON CONFLICT DO NOTHING;
        """
        conn.execute(text(query))
            conn.execute(text("DROP TABLE IF EXISTS temp_upsert"))

        if page >= data.get("total_pages", 1): break
        page += 1

def main():
    if not DB_URL:
        logging.error("DB_URL is missing! Check your .env file.")
        return

    engine = create_engine(DB_URL)
    to_date = datetime.now().strftime('%Y%m%d')
    from_date = (datetime.now() - timedelta(days=32)).strftime('%Y%m%d')

    for suffix, table in ENDPOINTS_MAP.items():
        try:
            sync_endpoint(engine, suffix, table, from_date, to_date)
        except Exception as e:
            logging.error(f"Failed to sync {table}: {e}")

if __name__ == "__main__":
    main()