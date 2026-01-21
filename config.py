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
        "company_url": "company_website",
        "date_of_establishment": "date_of_establishment",
        "founding_year": "year_of_founding",
        "update_date": "last_updated_date",
        "qualification_grade": "qualification_level"
    },
    "notification_certification_information_gbizinfo": {
        "corporate_number": "corporate_number",
        ######### TO REMOVE #########
        "name": ["name_corporate_number", "name"],
        "location": ["headquarters_address_corporate_number", "headquarters_address"],
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
        "operating_revenue2_summary_of_business_results": "operating_income",
        "operating_revenue2_summary_of_business_results_unit_ref": "operating_income_unit",
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