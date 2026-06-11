# Databricks notebook source
# MAGIC %md
# MAGIC # Generate Synthetic Audit/Tax PDFs → UC Volume
# MAGIC
# MAGIC In-workspace port of `generate_pdfs.py`. Builds 12 realistic synthetic
# MAGIC audit/tax/compliance PDFs **with no dependencies** (raw PDF bytes) and
# MAGIC writes them straight to the source volume's `raw_pdfs/` folder.
# MAGIC
# MAGIC Run this BEFORE the encryption notebook. All content is synthetic — fake
# MAGIC names, EINs, SSNs, and figures — safe to use in the AMEX sandbox.
# MAGIC
# MAGIC After this runs:  raw_pdfs/ → (encryption notebook) → encrypted_pdfs/ → pipeline.

# COMMAND ----------

# ── Set these to match config.py ─────────────────────────────────────────────
CATALOG = "unstructured_poc_v2"
SCHEMA  = "mason_demo_schema"
VOLUME  = "source_volume"
RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/raw_pdfs"
WRITE_EVAL_CSV = True   # also write KA eval Q/A pairs for evaluate_KA.py

# COMMAND ----------

import os
import csv
import textwrap


def make_pdf(title: str, sections: list, metadata: dict = None) -> bytes:
    """Minimal but valid single-page PDF with title + sections. No deps."""
    lines = []
    y = 750  # letter: 612x792 points

    def add_line(text, font_size=12, bold=False, indent=0):
        nonlocal y
        if y < 72:  # ran off the page — truncate for simplicity
            return
        font = "/F2" if bold else "/F1"
        text = (text.replace("—", "--").replace("–", "-")
                    .replace("‘", "'").replace("’", "'")
                    .replace("“", '"').replace("”", '"'))
        escaped = text.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")
        lines.append(f"BT {font} {font_size} Tf {72 + indent} {y} Td ({escaped}) Tj ET")
        y -= font_size + 4

    add_line(title, font_size=18, bold=True)
    y -= 10
    if metadata:
        add_line(" | ".join(f"{k}: {v}" for k, v in metadata.items()), font_size=9)
        y -= 5
    add_line("=" * 80, font_size=8)
    y -= 10

    for heading, body in sections:
        add_line(heading, font_size=14, bold=True)
        y -= 4
        for paragraph in body.split("\n"):
            wrapped = textwrap.wrap(paragraph, width=90)
            if not wrapped:
                y -= 12
                continue
            for line in wrapped:
                add_line(line, font_size=10, indent=10)
        y -= 8

    stream_content = "\n".join(lines)
    stream_bytes = stream_content.encode("latin-1")

    objects = [
        "1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj",
        "2 0 obj\n<< /Type /Pages /Kids [3 0 R] /Count 1 >>\nendobj",
        ("3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] "
         "/Contents 4 0 R /Resources << /Font << /F1 5 0 R /F2 6 0 R >> >> >>\nendobj"),
        (f"4 0 obj\n<< /Length {len(stream_bytes)} >>\nstream\n"
         f"{stream_content}\nendstream\nendobj"),
        "5 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>\nendobj",
        "6 0 obj\n<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>\nendobj",
    ]

    pdf_parts = ["%PDF-1.4\n"]
    offsets = []
    for obj in objects:
        offsets.append(len("".join(pdf_parts).encode("latin-1")))
        pdf_parts.append(obj + "\n")
    xref_offset = len("".join(pdf_parts).encode("latin-1"))
    pdf_parts.append("xref\n")
    pdf_parts.append(f"0 {len(objects) + 1}\n")
    pdf_parts.append("0000000000 65535 f \n")
    for off in offsets:
        pdf_parts.append(f"{off:010d} 00000 n \n")
    pdf_parts.append("trailer\n")
    pdf_parts.append(f"<< /Size {len(objects) + 1} /Root 1 0 R >>\n")
    pdf_parts.append("startxref\n")
    pdf_parts.append(f"{xref_offset}\n")
    pdf_parts.append("%%EOF\n")
    return "".join(pdf_parts).encode("latin-1")

# COMMAND ----------

DOCUMENTS = [
    {
        "filename": "idr_2025_q3_executive_compensation.pdf",
        "title": "IRS Information Document Request - Executive Compensation Review",
        "metadata": {"IDR Number": "IDR-2025-0847", "Tax Year": "2023", "Date Issued": "2025-07-15", "Examiner": "Patricia Morales, IRS LB&I"},
        "sections": [
            ("1. Background",
             "This Information Document Request (IDR) is issued pursuant to the examination of American Express Company (EIN: 13-4922250) for tax year 2023. The IRS Large Business & International Division is conducting a review of executive compensation arrangements under IRC Sections 162(m) and 409A.\n\nThe examination focuses on deferred compensation agreements, performance-based incentive structures, and supplemental executive retirement plans (SERPs) for the top five named executive officers."),
            ("2. Documents Requested",
             "a) All employment agreements, amendments, and side letters for Named Executive Officers (NEOs) effective during tax year 2023.\nb) Schedule of all deferred compensation balances as of December 31, 2023, including vesting schedules and distribution triggers.\nc) Board compensation committee minutes from January 2023 through December 2023 addressing executive pay decisions.\nd) Actuarial valuations for SERP obligations as of plan year-end.\ne) Section 162(m) analysis supporting the deductibility of performance-based compensation exceeding $1,000,000 per NEO."),
            ("3. Relevant IRC Provisions",
             "IRC Section 162(m) limits the deduction for compensation paid to covered employees to $1,000,000. Following the Tax Cuts and Jobs Act of 2017, the performance-based compensation exception was eliminated for tax years beginning after December 31, 2017, subject to transition relief for binding contracts in effect on November 2, 2017.\n\nIRC Section 409A imposes requirements on the timing of deferral elections and distributions from nonqualified deferred compensation plans. Failure to comply may result in immediate income inclusion plus a 20% additional tax."),
            ("4. Response Deadline",
             "Please provide the requested documents within 30 calendar days of receipt of this IDR. If additional time is needed, contact the examining agent to discuss an extension. Failure to respond may result in the issuance of a summons under IRC Section 7602."),
        ],
        "question": "What was the IRS's request regarding executive compensation for tax year 2023?",
        "guideline": "Answer should mention IDR-2025-0847, IRC Sections 162(m) and 409A, deferred compensation review, NEO employment agreements, and the 30-day response deadline."
    },
    {
        "filename": "amex_response_idr_exec_comp.pdf",
        "title": "American Express Response to IDR-2025-0847 - Executive Compensation",
        "metadata": {"Reference": "IDR-2025-0847", "Date": "2025-08-10", "Prepared By": "AMEX Tax Department", "Reviewed By": "Corporate Legal"},
        "sections": [
            ("1. Executive Summary",
             "American Express Company submits this response to IDR-2025-0847 regarding executive compensation for tax year 2023. All requested documents are enclosed. We confirm that all compensation arrangements comply with IRC Sections 162(m) and 409A requirements.\n\nTotal NEO compensation for tax year 2023 was $47.2M across five named executive officers. Of this amount, $12.8M represents performance-based incentive compensation tied to measurable financial metrics."),
            ("2. Employment Agreements",
             "Enclosed are employment agreements for the five NEOs: the Chairman & CEO, CFO, President of Global Consumer Services, President of Global Commercial Services, and General Counsel. Each agreement was executed prior to November 2, 2017, and has not been materially modified since that date.\n\nKey provisions include: base salary ranges from $1.0M to $1.7M, annual incentive target ranges from 200% to 400% of base salary, and long-term incentive grants valued between $5M and $15M at target."),
            ("3. Deferred Compensation Analysis",
             "As of December 31, 2023, aggregate deferred compensation balances for NEOs totaled $89.4M. All deferral elections were made in compliance with Section 409A timing requirements - initial deferral elections were made within 30 days of plan eligibility, and subsequent elections were made by December 31 of the year prior to the service period.\n\nDistribution triggers are limited to: separation from service (with 6-month delay for specified employees), disability, death, and change in control. No in-service distributions are permitted."),
            ("4. Section 162(m) Deductibility",
             "We have prepared a detailed analysis demonstrating that $23.1M of the total NEO compensation qualifies for the transition relief under the Tax Cuts and Jobs Act, as these amounts were payable under binding contracts in effect on November 2, 2017. The remaining $24.1M is subject to the $1M per-NEO deduction limitation.\n\nSchedule A (attached) provides a per-NEO breakdown of deductible and non-deductible compensation components."),
            ("5. SERP Valuations",
             "Actuarial valuations for the Supplemental Executive Retirement Plan were conducted by Towers Watson as of December 31, 2023. The projected benefit obligation for NEO participants totaled $156.2M, with a current-year service cost of $8.7M. Discount rate used: 5.25%. Mortality table: Pri-2012 with MP-2023 projection scale."),
        ],
        "question": "What was our response to the IRS's question about executive compensation?",
        "guideline": "Answer should reference the $47.2M total NEO compensation, the Section 162(m) analysis showing $23.1M qualifying for transition relief, the $89.4M deferred compensation balances, and compliance with Section 409A requirements."
    },
    {
        "filename": "transfer_pricing_study_2023.pdf",
        "title": "Transfer Pricing Documentation - Intercompany Services Arrangement",
        "metadata": {"Document Type": "Transfer Pricing Study", "Tax Year": "2023", "Jurisdiction": "United States / United Kingdom", "Prepared By": "AMEX Transfer Pricing Group"},
        "sections": [
            ("1. Overview of Intercompany Arrangement",
             "This transfer pricing study documents the arm's length nature of the intercompany services arrangement between American Express Company (US parent) and American Express Services Europe Limited (UK subsidiary). The arrangement covers management services, technology development, and shared services provided by the US parent to the UK subsidiary.\n\nTotal intercompany charges for tax year 2023: $342.8M (management fees: $127.3M, technology allocation: $158.9M, shared services: $56.6M)."),
            ("2. Functional Analysis",
             "The US parent performs the following functions: strategic management and oversight, technology development and maintenance (including the global card processing platform), human resources administration, financial planning and analysis, and legal/compliance support.\n\nThe UK subsidiary performs local market functions including: customer acquisition and retention for the UK/European market, merchant network development, regulatory compliance with FCA requirements, and local marketing campaigns."),
            ("3. Economic Analysis - Comparable Companies",
             "Using the Comparable Profits Method (CPM) as the primary method, we identified 12 comparable companies providing similar management and technology services. The interquartile range of operating margins for comparable companies was 8.2% to 14.7%, with a median of 11.3%.\n\nThe tested party (UK subsidiary) reported an operating margin of 10.8% for tax year 2023, which falls within the interquartile range of comparable companies, supporting the arm's length nature of the intercompany charges."),
            ("4. OECD Guidelines Compliance",
             "This documentation complies with the OECD Transfer Pricing Guidelines for Multinational Enterprises (2022 edition) and satisfies the requirements of both US Treasury Regulation Section 1.482 and UK transfer pricing rules under Part 4 of TIOPA 2010.\n\nA Master File and Local File have been prepared in accordance with OECD BEPS Action 13 Country-by-Country Reporting requirements."),
        ],
        "question": "What is the transfer pricing arrangement between AMEX US and the UK subsidiary?",
        "guideline": "Answer should mention the $342.8M total intercompany charges, the three service categories (management, technology, shared services), the CPM method, and the UK subsidiary's 10.8% operating margin within the arm's length range."
    },
    {
        "filename": "idr_2025_q4_digital_services_tax.pdf",
        "title": "IRS IDR - Digital Services Tax Credit Analysis",
        "metadata": {"IDR Number": "IDR-2025-1203", "Tax Year": "2023", "Date Issued": "2025-10-01", "Examiner": "Robert Chen, IRS LB&I"},
        "sections": [
            ("1. Request Overview",
             "This IDR relates to foreign tax credits claimed on the 2023 Form 1120 for Digital Services Taxes (DSTs) paid to the United Kingdom (2% DST), France (3% DST), and Italy (3% DST). The IRS seeks to determine whether these DSTs are creditable foreign income taxes under IRC Section 901 and the 2022 Final Foreign Tax Credit Regulations.\n\nTotal DST amounts paid: UK: $18.7M, France: $12.3M, Italy: $8.1M. Total claimed as foreign tax credits: $39.1M."),
            ("2. Documents Requested",
             "a) Calculation methodology for DST liability in each jurisdiction, including the revenue base and applicable rates.\nb) Analysis of whether each DST satisfies the net income tax requirement under Treas. Reg. 1.901-2(b).\nc) Documentation of the realization, gross receipts, and cost recovery requirements for each DST.\nd) Correspondence with foreign tax authorities regarding DST assessments or audit adjustments.\ne) Internal memoranda or external opinions addressing the creditability of DSTs under the 2022 Final Regulations."),
            ("3. Regulatory Framework",
             "The 2022 Final Foreign Tax Credit Regulations (TD 9959) established new requirements for foreign taxes to qualify as creditable income taxes. A foreign tax must satisfy: (1) the net income tax requirement, (2) the realization requirement, (3) the gross receipts requirement, and (4) the cost recovery requirement.\n\nThe applicability of these requirements to DSTs is an area of active regulatory scrutiny, particularly given that DSTs are typically imposed on gross revenues rather than net income."),
        ],
        "question": "What foreign tax credits did AMEX claim for Digital Services Taxes in 2023?",
        "guideline": "Answer should mention the $39.1M total DST credits claimed across UK ($18.7M), France ($12.3M), and Italy ($8.1M), and the 2022 Final Foreign Tax Credit Regulations creditability analysis."
    },
    {
        "filename": "soc_compliance_audit_2024.pdf",
        "title": "SOC 2 Type II Compliance Report - Tax Data Processing Controls",
        "metadata": {"Report Period": "Jan 1 - Dec 31, 2024", "Auditor": "Deloitte & Touche LLP", "System": "AMEX Tax Data Processing Platform"},
        "sections": [
            ("1. Scope and Objectives",
             "This SOC 2 Type II report covers the Tax Data Processing Platform operated by American Express Company's Enterprise Tax Department. The platform processes tax-related data for US federal, state, and international tax compliance, including Forms 1099, 1042-S, and country-by-country reporting.\n\nThe examination covers the Trust Services Criteria for Security, Availability, Processing Integrity, and Confidentiality for the period January 1, 2024 through December 31, 2024."),
            ("2. System Description",
             "The Tax Data Processing Platform consists of: (a) data ingestion pipelines collecting transaction data from 47 source systems across card member services, merchant services, and corporate treasury; (b) a tax calculation engine applying jurisdiction-specific rules for 130+ jurisdictions; (c) regulatory filing generation for IRS, state agencies, and foreign tax authorities; (d) a reconciliation framework ensuring data integrity across processing stages.\n\nThe platform processes approximately 2.3 billion transaction records annually and generates over 45 million tax information returns."),
            ("3. Control Activities and Test Results",
             "Control C-1 (Access Management): Logical access to the Tax Data Processing Platform is restricted to authorized personnel through multi-factor authentication and role-based access controls. Tested 4 quarters: No exceptions noted.\n\nControl C-2 (Change Management): All changes to tax calculation rules and filing templates follow a documented change management process including peer review, testing in a non-production environment, and management approval. Tested 4 quarters: 1 exception noted - a configuration change was deployed without documented test results in Q2 2024. Management remediated the control gap in Q3 2024.\n\nControl C-3 (Data Integrity): Automated reconciliation controls verify completeness and accuracy of tax data at each processing stage. Tested 4 quarters: No exceptions noted."),
            ("4. Auditor Opinion",
             "In our opinion, the controls within the Tax Data Processing Platform were suitably designed and operating effectively throughout the examination period to provide reasonable assurance that the applicable trust services criteria were met, subject to the exception noted in Control C-2."),
        ],
        "question": "What were the findings in the SOC 2 compliance audit for the tax data platform?",
        "guideline": "Answer should mention the SOC 2 Type II report covering Security, Availability, Processing Integrity, and Confidentiality, the 2.3 billion transactions processed, the exception in Control C-2 for change management in Q2 2024, and the overall positive auditor opinion."
    },
    {
        "filename": "fed_reserve_examination_response.pdf",
        "title": "Response to Federal Reserve Examination - Tax Reserve Methodology",
        "metadata": {"Reference": "FRB-EX-2025-AMX-004", "Date": "2025-09-15", "Division": "Enterprise Tax", "Regulator": "Federal Reserve Bank of New York"},
        "sections": [
            ("1. Background",
             "In connection with the Federal Reserve's annual supervisory examination of American Express Company as a Bank Holding Company, the examination team has requested documentation of the methodology used to establish tax reserves (uncertain tax positions) under ASC 740-10.\n\nAs of December 31, 2024, AMEX reported total unrecognized tax benefits of $2.8B on the consolidated balance sheet, an increase of $340M from the prior year."),
            ("2. Tax Reserve Methodology",
             "AMEX follows a two-step process for establishing tax reserves under ASC 740-10-25: (1) Recognition: A tax position is recognized only when it is more likely than not (>50% probability) that the position will be sustained upon examination. (2) Measurement: Recognized positions are measured at the largest amount of benefit that has a greater than 50% likelihood of being realized upon settlement.\n\nThe tax department maintains a database of approximately 850 individual tax positions across 45 jurisdictions, each supported by a technical memorandum and probability assessment."),
            ("3. Significant Tax Positions",
             "The five largest uncertain tax positions are: (1) Transfer pricing - intercompany services: $780M reserve, more-likely-than-not sustained at 65% confidence. (2) Foreign tax credit - DST creditability: $420M reserve, MLTN at 55% confidence. (3) State apportionment methodology: $310M reserve, MLTN at 70% confidence. (4) R&D credit - software development: $195M reserve, MLTN at 60% confidence. (5) GILTI high-tax exclusion: $145M reserve, MLTN at 75% confidence.\n\nThe increase in reserves is primarily attributable to the DST creditability position following the 2022 Final Foreign Tax Credit Regulations."),
            ("4. Governance and Oversight",
             "Tax reserves are reviewed quarterly by the Tax Reserve Committee (chaired by the VP of Tax) and presented to the Audit Committee of the Board of Directors semi-annually. External auditors (PricewaterhouseCoopers LLP) independently assess the reasonableness of tax positions as part of the annual financial statement audit."),
        ],
        "question": "How does AMEX calculate tax reserves for uncertain tax positions?",
        "guideline": "Answer should describe the ASC 740-10 two-step process (recognition at >50% probability, measurement at largest amount >50% likely), mention the $2.8B total reserves, and reference the top five positions including transfer pricing ($780M) and DST creditability ($420M)."
    },
    {
        "filename": "state_tax_audit_nexus_analysis.pdf",
        "title": "State Tax Nexus Analysis - Multi-State Apportionment Review",
        "metadata": {"Tax Years": "2021-2023", "States Under Audit": "California, New York, Illinois, Texas", "Status": "In Progress"},
        "sections": [
            ("1. Audit Overview",
             "Four states have initiated concurrent audits of American Express Company's state income/franchise tax returns for tax years 2021-2023. The primary issues under examination are: (a) the apportionment methodology for financial services income, (b) economic nexus thresholds for marketplace facilitator activities, and (c) the treatment of intercompany transactions in the combined/unitary group.\n\nPotential assessment exposure across all four states: $67.3M (before penalties and interest)."),
            ("2. California - Apportionment Methodology",
             "California's Franchise Tax Board (FTB) challenges AMEX's use of the single-sales-factor apportionment formula, asserting that certain card member fee income should be sourced to California based on the commercial domicile of the card member rather than the billing address.\n\nAMEX position: Card member fee income is properly sourced using the market-based sourcing rules under Cal. Rev. & Tax. Code Section 25136, with the benefit received by the card member at their billing address. Estimated California adjustment: $28.1M."),
            ("3. New York - Combined Reporting",
             "New York's Department of Taxation and Finance asserts that two AMEX subsidiaries not included in the combined group should be included based on unitary business principles. The subsidiaries operate digital payment processing services and were historically filed on a separate basis.\n\nAMEX position: The subsidiaries do not share sufficient factors of profitability (functional integration, centralized management, economies of scale) to meet New York's unitary business standard. Estimated New York adjustment: $19.7M."),
            ("4. Response Strategy",
             "The state tax team has engaged external counsel (KPMG) for the California and New York matters. Administrative protests will be filed within 60 days of any proposed assessments. The appeals process timeline is estimated at 18-24 months per state."),
        ],
        "question": "What states are currently auditing AMEX and what are the key issues?",
        "guideline": "Answer should mention California, New York, Illinois, and Texas, the $67.3M total exposure, California's apportionment challenge ($28.1M), New York's combined reporting dispute ($19.7M), and the engagement of KPMG as external counsel."
    },
    {
        "filename": "pillar_two_impact_assessment.pdf",
        "title": "OECD Pillar Two - Global Minimum Tax Impact Assessment",
        "metadata": {"Assessment Date": "2025-06-30", "Effective Date": "January 1, 2024", "Prepared By": "AMEX International Tax Planning"},
        "sections": [
            ("1. Overview of Pillar Two",
             "The OECD/G20 Inclusive Framework on Base Erosion and Profit Shifting (BEPS) adopted the Global Anti-Base Erosion (GloBE) Model Rules in December 2021, establishing a global minimum effective tax rate of 15% for multinational enterprises with consolidated revenues exceeding EUR 750 million.\n\nAmerican Express Company, with consolidated revenues of $60.5B for fiscal year 2024, is within scope of the GloBE rules in all jurisdictions that have enacted implementing legislation."),
            ("2. Jurisdictional Analysis",
             "AMEX operates in 130+ countries and territories. Based on our Pillar Two impact assessment, 8 jurisdictions have effective tax rates below 15% on a GloBE basis before application of substance-based income exclusions (SBIE):\n\nSingapore (ETR: 12.3%), Hong Kong (ETR: 11.8%), Ireland (ETR: 13.2%), UAE (ETR: 0% - pre-Corporate Tax Law), Bermuda (ETR: 0%), Cayman Islands (ETR: 0%), Luxembourg (ETR: 14.1%), Switzerland (ETR: 13.7%).\n\nAfter applying SBIE (5% of tangible assets + 5% of payroll), top-up tax exposure is estimated at $127M annually."),
            ("3. Compliance and Reporting Obligations",
             "AMEX will be required to file GloBE Information Returns (GIR) in each jurisdiction that has adopted the Income Inclusion Rule (IIR) or Qualified Domestic Minimum Top-up Tax (QDMTT). The first filing deadline for CY2024 is June 30, 2026.\n\nWe are implementing a Pillar Two compliance technology solution (Thomson Reuters ONESOURCE) to automate the jurisdictional ETR calculations, SBIE computations, and GIR preparation."),
        ],
        "question": "What is the estimated Pillar Two top-up tax exposure for AMEX?",
        "guideline": "Answer should mention the $127M annual top-up tax estimate, the 8 jurisdictions below 15% ETR, the substance-based income exclusion (SBIE), and the Thomson Reuters ONESOURCE implementation for compliance."
    },
    {
        "filename": "r_and_d_credit_documentation.pdf",
        "title": "Research & Development Tax Credit Study - Tax Year 2023",
        "metadata": {"IRC Section": "41", "Credit Claimed": "$287M", "Study Prepared By": "AMEX Tax / EY Advisory"},
        "sections": [
            ("1. Qualified Research Activities",
             "American Express claimed $287M in IRC Section 41 Research & Development tax credits for tax year 2023. The credits relate to qualified research expenditures (QREs) of $3.2B incurred across technology development activities including: (a) AI/ML model development for fraud detection and customer analytics, (b) cloud infrastructure engineering for the card processing platform migration, (c) mobile application development for digital wallet and contactless payment features, (d) cybersecurity research for threat detection and encryption protocols."),
            ("2. Four-Part Test Analysis",
             "Each qualified research activity satisfies the IRC Section 41(d) four-part test: (1) Permitted purpose: activities are undertaken to develop new or improved business components (products, processes, techniques, formulas, inventions, or software). (2) Technological uncertainty: activities involve uncertainty regarding capability, methodology, or appropriate design. (3) Process of experimentation: activities involve systematic evaluation of alternatives through modeling, simulation, or testing. (4) Technological in nature: activities rely on principles of engineering, computer science, or physical sciences."),
            ("3. QRE Breakdown by Department",
             "Technology Division: $2.1B (65.6% of total QREs) - primary contributor across AI/ML, cloud, and platform engineering.\nGlobal Consumer Services: $485M (15.2%) - mobile app development, digital wallet features.\nEnterprise Risk: $372M (11.6%) - fraud detection models, real-time transaction scoring.\nCybersecurity: $243M (7.6%) - encryption research, threat intelligence systems."),
            ("4. Documentation and Substantiation",
             "Each qualified activity is supported by: project-level technical narratives describing the technological uncertainty and experimentation, time tracking records for personnel performing qualified activities, financial records linking QREs to specific projects, and contemporaneous documentation per Treas. Reg. 1.41-4(d)."),
        ],
        "question": "How much R&D tax credit did AMEX claim and what activities qualified?",
        "guideline": "Answer should mention the $287M credit on $3.2B of QREs, the four qualifying activity areas (AI/ML, cloud, mobile, cybersecurity), the four-part test under IRC Section 41(d), and the Technology Division's 65.6% share of QREs."
    },
    {
        "filename": "tax_controversy_settlement_memo.pdf",
        "title": "Settlement Memorandum - IRS Appeals Conference FY2021-2022",
        "metadata": {"Case": "IRS Appeals #2025-LBI-AMX-0091", "Tax Years": "2021-2022", "Settlement Date": "2025-11-01", "Amount": "$892M resolved"},
        "sections": [
            ("1. Settlement Summary",
             "On November 1, 2025, American Express Company reached a comprehensive settlement with IRS Appeals resolving all issues under examination for tax years 2021-2022. The settlement resolves $892M in proposed adjustments, resulting in a net cash payment of $156M (including interest) and a $340M reduction in uncertain tax position reserves.\n\nThis settlement closes the 2021-2022 examination cycle and positions AMEX favorably for the 2023 examination currently in progress."),
            ("2. Issues Resolved",
             "Issue 1 - Transfer Pricing (Intercompany Services): Proposed adjustment of $415M. Settled at 35% of proposed amount ($145M). The settlement establishes an agreed-upon profit split methodology for future years, reducing future controversy risk.\n\nIssue 2 - Foreign Tax Credits (Timing): Proposed adjustment of $228M. Settled at 50% of proposed amount ($114M). The settlement confirms AMEX's credit versus deduction election methodology for foreign withholding taxes.\n\nIssue 3 - Capitalization vs. Expense (Software): Proposed adjustment of $167M. Conceded by IRS (settled at $0). AMEX's treatment of cloud computing costs as currently deductible under Rev. Proc. 2000-50 was accepted.\n\nIssue 4 - State Tax Deductions: Proposed adjustment of $82M. Settled at 75% of proposed amount ($62M). Technical dispute over timing of state tax accruals."),
            ("3. Financial Impact",
             "The settlement results in: (a) cash tax payment of $156M (including $23M interest), (b) reduction of uncertain tax position reserves by $340M, (c) effective tax rate benefit of 1.2 percentage points in Q4 2025, (d) avoidance of potential penalties of $45M that were proposed in the RAR.\n\nThe Tax Reserve Committee has approved the release of reserves and the financial impact will be reflected in Q4 2025 financial statements."),
        ],
        "question": "What was the outcome of the IRS Appeals settlement for 2021-2022?",
        "guideline": "Answer should describe the $892M in resolved adjustments, the $156M cash payment, the $340M reserve reduction, and the specific resolution of each issue - especially the IRS concession on software capitalization."
    },
    {
        "filename": "quarterly_tax_provision_q4_2025.pdf",
        "title": "Quarterly Tax Provision Workpaper - Q4 2025",
        "metadata": {"Period": "Q4 2025", "ETR": "22.1%", "Status": "Draft - Pending Audit Committee Review"},
        "sections": [
            ("1. Effective Tax Rate Summary",
             "For Q4 2025, American Express Company reports a consolidated effective tax rate (ETR) of 22.1%, compared to 23.4% in Q4 2024. The year-to-date ETR is 22.8%, within the guided range of 22-24%.\n\nThe Q4 2025 ETR reflects the favorable impact of the IRS Appeals settlement ($340M reserve release, contributing a 1.2pp ETR benefit) partially offset by Pillar Two top-up tax accruals ($31M)."),
            ("2. Rate Reconciliation",
             "Statutory US federal rate: 21.0%\nState and local taxes (net of federal benefit): +2.3%\nForeign rate differential: -1.8%\nR&D tax credits: -1.5%\nPillar Two top-up tax: +0.4%\nIRS settlement reserve release: -1.2%\nExec compensation (162(m) limitation): +0.8%\nForeign-derived intangible income (FDII): -0.7%\nOther permanent items: +0.3%\nStock-based compensation (ASC 718): -0.5%\nValuation allowance changes: +0.0%\n\nReported ETR: 22.1% (vs. prior year 23.4%)"),
            ("3. Deferred Tax Summary",
             "Net deferred tax assets as of December 31, 2025: $4.1B. Largest components: card member loan loss reserves ($1.8B DTA), depreciation and amortization ($920M DTL), stock-based compensation ($340M DTA), unrecognized tax benefits ($280M DTA), foreign tax credit carryforwards ($195M DTA).\n\nNo valuation allowance is required - management has concluded that it is more likely than not that all deferred tax assets will be realized based on projected future taxable income."),
        ],
        "question": "What is AMEX's effective tax rate for Q4 2025 and what drives it?",
        "guideline": "Answer should state the 22.1% ETR, explain the key drivers (IRS settlement -1.2pp, R&D credits -1.5pp, Pillar Two +0.4pp, state taxes +2.3%), and note the $4.1B net deferred tax assets."
    },
    {
        "filename": "audit_response_policy_manual.pdf",
        "title": "AMEX Tax Department - Audit Response Policy and Procedures Manual",
        "metadata": {"Version": "4.2", "Effective Date": "2025-01-01", "Owner": "VP, Tax Controversy"},
        "sections": [
            ("1. Purpose and Scope",
             "This manual establishes the policies and procedures for responding to tax audits and examinations by federal, state, and international tax authorities. It applies to all personnel in the American Express Enterprise Tax Department and external advisors engaged in audit defense.\n\nThe manual covers: initial audit notification processing, document retention and production, IDR response protocols, taxpayer position development, appeals and settlement procedures, and post-audit remediation."),
            ("2. IDR Response Protocol",
             "Upon receipt of an IDR: (1) Log the IDR in the Tax Controversy Management System within 24 hours. (2) Assign a lead analyst based on the subject matter. (3) Schedule a kickoff meeting with the lead analyst, tax controversy manager, and relevant subject matter experts within 3 business days. (4) Prepare a response timeline - default 30 days from receipt. (5) Draft responses undergo two levels of review: technical review by a tax director and legal privilege review by Corporate Legal. (6) Final responses are approved by the VP, Tax Controversy before submission."),
            ("3. Document Production Guidelines",
             "All documents produced to tax authorities must be reviewed for: (a) attorney-client privilege - work product or communications with legal counsel must be withheld or redacted; (b) relevance - only documents responsive to the specific IDR request should be produced; (c) confidentiality - PII including Social Security numbers, individual compensation amounts, and customer data must be redacted unless specifically requested and legally required; (d) consistency - ensure documents are consistent with prior productions and publicly filed returns."),
            ("4. Escalation Procedures",
             "Escalation to the SVP, Tax and/or General Counsel is required when: (a) proposed adjustments exceed $50M for a single issue, (b) the examiner requests documents that may be subject to attorney-client privilege, (c) the examining agent threatens summons enforcement, (d) the audit involves coordination with DOJ Tax Division or criminal investigation, (e) settlement negotiations involve concessions that would establish unfavorable precedent for future tax years."),
        ],
        "question": "What is the standard IDR response process at AMEX?",
        "guideline": "Answer should describe the 6-step IDR protocol: logging within 24 hours, assigning a lead analyst, kickoff meeting within 3 business days, 30-day response timeline, two-level review (technical + legal), and VP approval before submission."
    },

    # ── HR domain (3) — filename ends with _hr to drive filename-based classification ──
    {
        "filename": "executive_offer_letter_hr.pdf",
        "title": "Employment Offer Letter & Onboarding Packet",
        "metadata": {"Document Type": "hr", "Req ID": "REQ-2026-04471", "Date": "2026-02-09", "HR Business Partner": "Dana Whitfield"},
        "sections": [
            ("1. Candidate Record (Confidential)",
             "Candidate: Marcus T. Halloran\nHome Address: 4827 Camelback Road, Apt 12B, Phoenix, AZ 85018\nPersonal Email: marcus.halloran84@gmail.com\nMobile: (602) 555-0173\nSocial Security Number: 412-55-9087\nProposed Start Date: 2026-03-02\nWork Email (provisioned): marcus.halloran@aexp.com\n\nHR Business Partner: Dana Whitfield -- dana.whitfield@aexp.com -- (212) 555-0149."),
            ("2. Position & Compensation",
             "Title: Senior Manager, Global Decision Science. Level: Band 35. Reporting to: Amber Gupta, EVP, Global Decision Science.\n\nBase salary: $198,000 annually, paid semi-monthly. Target annual incentive: 25% of base. Sign-on bonus: $40,000, payable in the first full pay cycle, subject to a 12-month clawback. Long-term incentive grant: $120,000 in restricted stock units, vesting 25% annually over four years."),
            ("3. Onboarding & Acknowledgement",
             "This offer is contingent on successful completion of a background check and I-9 employment eligibility verification. Please return the signed acceptance and direct-deposit enrollment form to the HR Business Partner above within five business days. Benefits enrollment opens on your start date with a 31-day election window. Questions regarding this offer should be directed to Dana Whitfield at the contact details in Section 1."),
        ],
        "question": "What are the compensation terms in Marcus Halloran's offer letter?",
        "guideline": "Answer should mention the Senior Manager, Global Decision Science role, $198,000 base salary, 25% target incentive, $40,000 sign-on bonus with 12-month clawback, and the $120,000 RSU grant vesting over four years."
    },
    {
        "filename": "benefits_enrollment_confirmation_hr.pdf",
        "title": "2026 Benefits Enrollment Confirmation",
        "metadata": {"Document Type": "hr", "Plan Year": "2026", "Confirmation No": "BEN-2026-338021", "Date": "2026-01-18"},
        "sections": [
            ("1. Employee Record (Confidential)",
             "Employee: Priya Nair\nEmployee ID: 1184502\nMailing Address: 1290 Avenue of the Americas, Unit 705, New York, NY 10104\nPersonal Email: priya.nair.home@outlook.com\nPhone: (917) 555-0162\nSocial Security Number: 287-41-6635\nDate of Birth: 1989-07-22\n\nBenefits Service Center: benefits@aexp.com -- (800) 555-0118."),
            ("2. Elected Plans",
             "Medical: Premier PPO (employee + spouse). Annual employee premium: $3,840. Dental: Comprehensive Plan A. Vision: Standard. Health Savings Account contribution: $2,400 employee + $1,200 employer match.\n\nLife insurance: 2x base salary (company-paid) plus $200,000 supplemental (employee-paid). 401(k): 8% employee deferral with 6% company match. Dependent: Spouse -- Anil Nair (DOB 1987-03-14)."),
            ("3. Confirmation & Next Steps",
             "These elections are effective January 1, 2026 through December 31, 2026 and are irrevocable absent a qualifying life event. Review the beneficiary designations on file and update them within 30 days if changes are needed. Direct any discrepancies to the Benefits Service Center listed in Section 1 referencing confirmation number BEN-2026-338021."),
        ],
        "question": "What benefits did Priya Nair elect for the 2026 plan year?",
        "guideline": "Answer should mention the Premier PPO medical (employee + spouse) with a $3,840 annual premium, the $2,400 HSA contribution with $1,200 employer match, 2x base company-paid life plus $200,000 supplemental, and the 8% 401(k) deferral with 6% match."
    },
    {
        "filename": "payroll_direct_deposit_change_hr.pdf",
        "title": "Payroll Direct Deposit Change Authorization",
        "metadata": {"Document Type": "hr", "Form": "PAY-DD-07", "Effective Pay Date": "2026-03-15", "Date Submitted": "2026-03-04"},
        "sections": [
            ("1. Employee & Account Details (Confidential)",
             "Employee: Devon R. Castellano\nEmployee ID: 1170338\nResidential Address: 88 Tremont Street, Apt 3F, Boston, MA 02108\nWork Email: devon.castellano@aexp.com\nPhone: (617) 555-0155\nSocial Security Number: 501-22-7744\n\nNew Bank: Liberty Federal Credit Union. Routing Number: 211381990. Account Number: 6620154398. Account Type: Checking. Allocation: 100% of net pay."),
            ("2. Prior Account & Authorization",
             "Prior deposit account ending 4471 will be deactivated effective the pay date above. I authorize American Express to deposit my net pay to the account listed in Section 1 and, if necessary, to reverse any deposit made in error. This authorization remains in effect until I submit a superseding form.\n\nSubmitted by: Devon R. Castellano. Verified by Payroll Operations: Karen Liu -- karen.liu@aexp.com -- (212) 555-0137."),
            ("3. Processing Notes",
             "A pre-note verification will run on the next payroll cycle; the change takes full effect on the effective pay date of March 15, 2026. Employees should retain a copy for their records and report any unexpected deposit activity to Payroll Operations immediately at the contact in Section 2."),
        ],
        "question": "What direct deposit change did Devon Castellano authorize?",
        "guideline": "Answer should mention the switch to Liberty Federal Credit Union checking (routing 211381990, account 6620154398) at 100% of net pay, the deactivation of the prior account ending 4471, and the March 15, 2026 effective pay date."
    },

    # ── Engineering domain (3) — filename ends with _engineering ──
    {
        "filename": "auth_service_outage_postmortem_engineering.pdf",
        "title": "Incident Postmortem -- Auth Service Outage",
        "metadata": {"Document Type": "engineering", "Incident": "INC-2026-0419", "Severity": "SEV1", "Date": "2026-02-27"},
        "sections": [
            ("1. Incident Summary & Responders",
             "Incident Commander: Rachel Okonkwo -- rachel.okonkwo@aexp.com -- (480) 555-0191\nPrimary On-Call (Auth): Sameer Patel -- sameer.patel@aexp.com -- (480) 555-0124\nComms Lead: Tony Briggs -- tony.briggs@aexp.com -- (212) 555-0188\n\nIncident INC-2026-0419 (SEV1) caused a 47-minute partial outage of the cardmember authentication service on 2026-02-26, 14:12-14:59 ET. Approximately 6.2% of login attempts failed during the window."),
            ("2. Root Cause",
             "A configuration change to the OAuth token-signing service deployed a rotated signing key without propagating the public key to all validation nodes. Validation nodes in the us-east region rejected tokens signed with the new key, producing HTTP 401 responses for affected sessions.\n\nThe change passed staging because the staging cluster shares a single key-distribution node, masking the propagation gap that exists in the multi-region production topology."),
            ("3. Resolution & Action Items",
             "Mitigation: rolled back the key rotation at 14:51 ET; full recovery by 14:59 ET. Action items: (1) add a cross-region key-propagation health check to the deploy pipeline -- owner Sameer Patel, due 2026-03-13; (2) require a canary in a multi-node validation cluster before production rollout -- owner Rachel Okonkwo, due 2026-03-20; (3) add alerting on 401-rate deltas per region -- owner Tony Briggs, due 2026-03-06."),
        ],
        "question": "What caused the auth service outage in incident INC-2026-0419?",
        "guideline": "Answer should explain that a rotated OAuth token-signing key was deployed without propagating the public key to all us-east validation nodes, causing 401 rejections, that staging masked the gap with a single key-distribution node, and that rollback restored service within 47 minutes."
    },
    {
        "filename": "card_platform_oncall_runbook_engineering.pdf",
        "title": "Card Platform On-Call Runbook & Escalation Roster",
        "metadata": {"Document Type": "engineering", "Service": "Card Authorization Platform", "Version": "3.1", "Updated": "2026-02-15"},
        "sections": [
            ("1. Escalation Roster",
             "Primary On-Call: Lena Vasquez -- lena.vasquez@aexp.com -- (480) 555-0166\nSecondary On-Call: Omar Haddad -- omar.haddad@aexp.com -- (480) 555-0177\nEngineering Manager: Krishna Madhav Reddy -- (602) 555-0143\nPlatform Director (escalation after 30 min SEV1): Abhishek Dixit -- (602) 555-0109\n\nPagerDuty rotation: card-platform-primary. Escalation policy CP-ESC-2."),
            ("2. Common Alerts & First Response",
             "ALERT authz_latency_p99 > 800ms: check the downstream risk-scoring service health dashboard; if risk-scoring is degraded, enable the cached-decision fallback flag `authz.fallback.enabled=true`.\n\nALERT decline_rate_spike: compare against the merchant-category baseline; a spike isolated to one acquirer usually indicates an upstream network issue -- open a bridge with Network Operations before changing platform config.\n\nALERT token_vault_unreachable: this is an automatic SEV1; page the secondary immediately and notify the Engineering Manager in Section 1."),
            ("3. Recovery Procedures",
             "Restart sequence for the authorization workers must follow: drain connections, wait for in-flight transactions to settle (max 30s), then rolling-restart no more than two pods at a time to preserve quorum. Never restart the token vault and the authorization workers simultaneously. After recovery, confirm the p99 latency returns below 400ms and post an all-clear in the incident channel. For anything exceeding 30 minutes at SEV1, escalate to the Platform Director listed in Section 1."),
        ],
        "question": "What is the first response for an authz_latency_p99 alert on the card platform?",
        "guideline": "Answer should describe checking the downstream risk-scoring service health and, if it is degraded, enabling the cached-decision fallback flag authz.fallback.enabled=true, and reference the escalation roster for SEV1 handling."
    },
    {
        "filename": "tokenization_service_system_design_engineering.pdf",
        "title": "System Design -- Card Tokenization Service",
        "metadata": {"Document Type": "engineering", "Design Doc": "DD-2026-0072", "Status": "Approved", "Author": "Sai Prasad Devarakonda"},
        "sections": [
            ("1. Authors & Reviewers",
             "Author: Sai Prasad Devarakonda -- sai.devarakonda@aexp.com -- (602) 555-0198\nReviewer (Security): Abhijeet Gandharwar -- abhijeet.gandharwar@aexp.com -- (212) 555-0115\nReviewer (Platform): Chris Patterson -- chris.patterson@aexp.com -- (480) 555-0181\n\nThis document describes the design of a service that replaces raw primary account numbers (PANs) with format-preserving tokens. Test PAN used in examples: 3782 822463 10005 (synthetic AmEx test number -- not a live card)."),
            ("2. Token Generation & Vault",
             "The service accepts a PAN, validates it against the issuer BIN range, and generates a surrogate token preserving the last four digits for display (e.g., token maps card 3782 822463 10005 to display value ****-****-*-0005). Tokens are random, non-reversible without the vault, and stored in a partitioned key-value vault encrypted with envelope encryption (per-record data key wrapped by a KMS master key).\n\nDetokenization requires a mutually authenticated mTLS call plus a scoped service identity; no batch detokenization is permitted."),
            ("3. Compliance & Throughput",
             "The design keeps raw PANs out of all downstream analytics stores, narrowing PCI-DSS scope to the vault and tokenization service. Target throughput: 45,000 tokenizations/sec at p99 < 12ms. Keys rotate every 90 days with overlapping validity to avoid the propagation failure mode seen in INC-2026-0419. Security sign-off is required before any change to the detokenization path."),
        ],
        "question": "How does the card tokenization service protect primary account numbers?",
        "guideline": "Answer should explain that PANs are replaced with random, non-reversible surrogate tokens preserving the last four digits, stored in a vault using envelope encryption with a KMS master key, that detokenization requires mutually authenticated mTLS with a scoped identity and no batch detokenization, and that this narrows PCI-DSS scope."
    },

    # ── Support domain (3) — filename ends with _support ──
    {
        "filename": "cardmember_dispute_case_support.pdf",
        "title": "Card Member Dispute Resolution Case File",
        "metadata": {"Document Type": "support", "Case": "DSP-2026-558204", "Opened": "2026-02-20", "Analyst": "Gabriela Mendez"},
        "sections": [
            ("1. Card Member Record (Confidential)",
             "Card Member: Harold W. Pemberton\nAccount (card): 3782 100045 21009\nBilling Address: 720 Lakeshore Drive, Apt 18C, Chicago, IL 60611\nEmail: hwpemberton@example.com\nPhone: (312) 555-0148\n\nCase analyst: Gabriela Mendez -- gabriela.mendez@aexp.com -- (623) 555-0192. Case DSP-2026-558204."),
            ("2. Disputed Transaction",
             "Transaction date: 2026-02-11. Merchant: Horizon Travel Partners LLC. Amount: $2,340.00. Reason code: 4853 -- services not rendered. The card member states a flight package was cancelled by the merchant and no refund was issued after 21 days.\n\nProvisional credit of $2,340.00 was issued to the account on 2026-02-21 pending investigation. Card member contacted via phone in Section 1 to confirm receipt of the provisional credit."),
            ("3. Resolution Path",
             "A chargeback was initiated against the merchant acquirer under reason code 4853. The merchant has 30 days to respond with compelling evidence. If no valid representment is received by 2026-03-23, the provisional credit becomes permanent. The card member was advised to retain the cancellation confirmation email and was given the case number for status inquiries via the analyst contact in Section 1."),
        ],
        "question": "How was Harold Pemberton's disputed transaction handled?",
        "guideline": "Answer should mention the $2,340.00 disputed Horizon Travel Partners charge under reason code 4853 (services not rendered), the provisional credit issued on 2026-02-21, the chargeback against the merchant acquirer, and that the credit becomes permanent if no representment is received by 2026-03-23."
    },
    {
        "filename": "fraud_hold_escalation_ticket_support.pdf",
        "title": "Support Escalation Ticket -- Fraud Hold Review",
        "metadata": {"Document Type": "support", "Ticket": "ESC-2026-771140", "Priority": "High", "Opened": "2026-03-01"},
        "sections": [
            ("1. Card Member & Identity Record (Confidential)",
             "Card Member: Yusuf A. Rahman\nAccount (card): 3782 100061 33007\nResidential Address: 215 Maple Court, Edison, NJ 08820\nEmail: yusuf.rahman@example.com\nPhone: (732) 555-0159\nSocial Security Number (last verified on file): 623-90-4471\n\nAgent: Tara Singh -- tara.singh@aexp.com -- (623) 555-0133. Ticket ESC-2026-771140."),
            ("2. Reason for Hold",
             "An automated fraud rule placed a temporary hold on the account after three high-value transactions in two countries within four hours: a $4,210 charge in London, a $3,775 charge in Dubai, and a $5,090 charge in New York on 2026-02-28. The velocity and geography pattern triggered rule FR-VEL-09.\n\nThe card member called to confirm that the London and Dubai charges were fraudulent and the New York charge was legitimate."),
            ("3. Escalation & Actions",
             "Verified identity using two-factor knowledge questions and the SSN on file in Section 1. Actions: (1) confirmed New York charge as valid and released it; (2) reversed the London and Dubai charges totaling $7,985 as fraud; (3) closed the compromised card ending 3007 and ordered a replacement to the address in Section 1; (4) escalated to the Fraud Operations team for pattern analysis. Replacement card expected within 5-7 business days."),
        ],
        "question": "Why was a fraud hold placed on Yusuf Rahman's account and how was it resolved?",
        "guideline": "Answer should explain that rule FR-VEL-09 triggered on three high-value charges across London, Dubai, and New York within four hours, that the London and Dubai charges totaling $7,985 were reversed as fraud while the New York charge was released as valid, and that the compromised card ending 3007 was closed and replaced."
    },
    {
        "filename": "chargeback_correspondence_support.pdf",
        "title": "Chargeback Correspondence -- Merchant & Card Member",
        "metadata": {"Document Type": "support", "Case": "CBK-2026-094417", "Stage": "Representment Review", "Date": "2026-03-05"},
        "sections": [
            ("1. Parties (Confidential)",
             "Card Member: Eleanor J. Whitcombe\nAccount (card): 3782 100078 44005\nBilling Address: 9 Rosewood Lane, Asheville, NC 28801\nEmail: ejwhitcombe@example.com\nPhone: (828) 555-0146\n\nMerchant: Summit Outdoor Gear Inc. Merchant contact: billing@summitoutdoor.example -- (704) 555-0171. Case handler: Marcus Lee -- marcus.lee@aexp.com -- (623) 555-0184."),
            ("2. Chargeback History",
             "Original transaction: 2026-01-29, $689.95, reason code 4534 -- duplicate processing. The card member reported being billed twice for a single order (order #SOG-44821). A chargeback for the duplicate $689.95 was filed on 2026-02-04.\n\nThe merchant submitted representment on 2026-02-26 asserting two distinct orders shipped. Supporting documents: two shipping confirmations and a signed delivery receipt."),
            ("3. Decision & Communication",
             "On review, the two shipping confirmations reference the same tracking number, supporting the card member's duplicate-charge claim. Decision: representment rejected; the $689.95 chargeback stands and the credit to the card member is final. The merchant was notified at the contact in Section 1 and advised of pre-arbitration rights. The card member was sent a resolution letter confirming the permanent credit and the case number for reference."),
        ],
        "question": "How was Eleanor Whitcombe's duplicate-charge chargeback resolved?",
        "guideline": "Answer should mention the $689.95 duplicate charge under reason code 4534, the merchant's representment claiming two distinct orders, the finding that both shipping confirmations referenced the same tracking number, and the decision to reject the representment so the card member's credit is final."
    },
]

# COMMAND ----------

# UC Volumes don't support direct POSIX makedirs / writes on serverless
# (os.makedirs -> errno 95; open(...,"wb") can yield empty files). Write to
# local disk first, then copy onto the volume with dbutils.fs.cp.
import os
_local = "/tmp/synthetic_pdfs"
os.makedirs(_local, exist_ok=True)

for doc in DOCUMENTS:
    pdf_bytes = make_pdf(doc["title"], doc["sections"], doc.get("metadata"))
    lp = f"{_local}/{doc['filename']}"
    with open(lp, "wb") as f:
        f.write(pdf_bytes)
    dbutils.fs.cp(f"file:{lp}", f"{RAW_DIR}/{doc['filename']}")
    print(f"  wrote {doc['filename']} ({len(pdf_bytes):,} B)")

print(f"\n{len(DOCUMENTS)} synthetic PDFs -> {RAW_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## (Optional) KA evaluation Q/A pairs
# MAGIC Writes a CSV of question + expected-answer guidelines for `evaluate_KA.py`.

# COMMAND ----------

if WRITE_EVAL_CSV:
    local_csv = "/tmp/eval_questions.csv"
    with open(local_csv, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["filename", "question", "answer"])  # header must be "answer" for evaluate_KA.py
        for doc in DOCUMENTS:
            w.writerow([doc["filename"], doc["question"], doc["guideline"]])
    eval_path = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/ka_eval/eval_questions.csv"
    dbutils.fs.cp(f"file:{local_csv}", eval_path)
    print(f"Wrote {len(DOCUMENTS)} eval rows to {eval_path}")
    for doc in DOCUMENTS:
        print(f"\nQ: {doc['question']}\n   ({doc['filename']})")
