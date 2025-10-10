import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys
import logging
import json
import subprocess

# Configuration
PROJECT_DIR = '/opt/airflow'

# Default arguments
default_args = {
    'owner': 'fintrust_analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Initialize DAG
dag = DAG(
    'dow30_earnings_docker',
    default_args=default_args,
    description='Dow 30 earnings collection with Docling parsing (local storage)',
    schedule_interval='0 2 * * 1',  # Weekly on Mondays at 2 AM
    catchup=False,
    tags=['earnings', 'dow30', 'docker', 'orchestrator', 'docling'],
)


def run_orchestrator(**context):
    """Run the orchestrator to process all Dow 30 companies"""
    logging.info("Starting orchestrator for Dow 30 earnings pipeline")
    
    try:
        result = subprocess.run(
            ['python', f'{PROJECT_DIR}/orchestrator.py'],
            capture_output=True,
            text=True,
            timeout=7200,  # 2 hours timeout (since it processes all companies)
            cwd=PROJECT_DIR
        )
        
        if result.returncode == 0:
            logging.info("Orchestrator completed successfully")
            if result.stdout:
                logging.info(f"Output:\n{result.stdout}")
            return {'status': 'success', 'message': 'Pipeline completed'}
        else:
            logging.error(f"Orchestrator failed: {result.stderr}")
            raise Exception(f"Orchestrator failed: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logging.error("Orchestrator timed out after 2 hours")
        raise
    except Exception as e:
        logging.error(f"Error running orchestrator: {str(e)}")
        raise


def run_docling_parser(**context):
    """Run Docling parser to extract structured data from PDFs"""
    logging.info("Starting Docling parser for downloaded PDFs")
    
    try:
        # Run docling_runner.py
        result = subprocess.run(
            [
                'python', f'{PROJECT_DIR}/docling_runner.py',
                '--input', f'{PROJECT_DIR}/downloads',
                '--output', f'{PROJECT_DIR}/data/parsed/docling',
                '--recursive'
            ],
            capture_output=True,
            text=True,
            timeout=3600,  # 1 hour timeout
            cwd=PROJECT_DIR
        )
        
        if result.returncode == 0:
            logging.info("Docling parser completed successfully")
            if result.stdout:
                logging.info(f"Output:\n{result.stdout}")
            return {'status': 'success', 'message': 'Docling parsing completed'}
        else:
            logging.error(f"Docling parser failed: {result.stderr}")
            raise Exception(f"Docling parser failed: {result.stderr}")
            
    except subprocess.TimeoutExpired:
        logging.error("Docling parser timed out after 1 hour")
        raise
    except Exception as e:
        logging.error(f"Error running Docling parser: {str(e)}")
        raise


def verify_outputs(**context):
    """Verify that outputs were created successfully"""
    logging.info("Verifying output files")
    
    try:
        # Check all output directories
        ir_links_dir = f'{PROJECT_DIR}/ir_links'
        extracted_dir = f'{PROJECT_DIR}/extracted_reports'
        downloads_dir = f'{PROJECT_DIR}/downloads'
        docling_dir = f'{PROJECT_DIR}/data/parsed/docling'
        logs_dir = f'{PROJECT_DIR}/logs'
        
        # Count files in each directory
        ir_links_files = []
        extracted_files = []
        download_files = []
        docling_json_files = []
        docling_table_files = []
        
        if os.path.exists(ir_links_dir):
            for root, dirs, files in os.walk(ir_links_dir):
                ir_links_files.extend([os.path.join(root, f) for f in files])
        
        if os.path.exists(extracted_dir):
            for root, dirs, files in os.walk(extracted_dir):
                extracted_files.extend([os.path.join(root, f) for f in files])
        
        if os.path.exists(downloads_dir):
            for root, dirs, files in os.walk(downloads_dir):
                download_files.extend([os.path.join(root, f) for f in files])
        
        if os.path.exists(docling_dir):
            for root, dirs, files in os.walk(docling_dir):
                if file.endswith('.json'):
                    docling_json_files.append(os.path.join(root, file))
                elif file.endswith('.csv') and 'table' in file:
                    docling_table_files.append(os.path.join(root, file))
        
        # Log results
        logging.info(f"IR links directory exists: {os.path.exists(ir_links_dir)}")
        logging.info(f"Extracted reports directory exists: {os.path.exists(extracted_dir)}")
        logging.info(f"Downloads directory exists: {os.path.exists(downloads_dir)}")
        logging.info(f"Docling parsed directory exists: {os.path.exists(docling_dir)}")
        logging.info(f"Files in ir_links: {len(ir_links_files)}")
        logging.info(f"Files in extracted_reports: {len(extracted_files)}")
        logging.info(f"Files in downloads: {len(download_files)}")
        logging.info(f"Docling JSON files: {len(docling_json_files)}")
        logging.info(f"Docling extracted tables: {len(docling_table_files)}")
        
        if download_files:
            logging.info("Sample downloaded files:")
            for file in download_files[:10]:  # Show first 10 files
                logging.info(f"   - {file}")
        
        if docling_json_files:
            logging.info("Sample Docling parsed files:")
            for file in docling_json_files[:5]:  # Show first 5 files
                logging.info(f"   - {file}")
        
        return {
            'status': 'success',
            'ir_links_count': len(ir_links_files),
            'extracted_count': len(extracted_files),
            'download_count': len(download_files),
            'docling_json_count': len(docling_json_files),
            'docling_table_count': len(docling_table_files)
        }
        
    except Exception as e:
        logging.error(f"Error verifying outputs: {str(e)}")
        return {'status': 'error', 'message': str(e)}


def generate_report(**context):
    """Generate completion report"""
    logging.info("Generating completion report")
    
    try:
        ti = context['task_instance']
        
        # Pull results from previous tasks
        orchestrator_result = ti.xcom_pull(task_ids='run_orchestrator')
        docling_result = ti.xcom_pull(task_ids='run_docling_parser')
        verify_result = ti.xcom_pull(task_ids='verify_outputs')
        
        # Get current quarter
        now = datetime.now()
        quarter = f"{now.year}_Q{(now.month-1)//3 + 1}"
        
        # Create summary report
        report = {
            'pipeline_run_date': datetime.now().isoformat(),
            'quarter': quarter,
            'orchestrator_status': orchestrator_result.get('status') if orchestrator_result else 'unknown',
            'docling_parser_status': docling_result.get('status') if docling_result else 'unknown',
            'verify_status': verify_result.get('status') if verify_result else 'unknown',
            'ir_links_files': verify_result.get('ir_links_count', 0) if verify_result else 0,
            'extracted_files': verify_result.get('extracted_count', 0) if verify_result else 0,
            'downloaded_files': verify_result.get('download_count', 0) if verify_result else 0,
            'docling_json_files': verify_result.get('docling_json_count', 0) if verify_result else 0,
            'docling_table_files': verify_result.get('docling_table_count', 0) if verify_result else 0,
        }
        
        logging.info("=" * 60)
        logging.info("PIPELINE EXECUTION SUMMARY")
        logging.info("=" * 60)
        logging.info(json.dumps(report, indent=2))
        logging.info("=" * 60)
        
        # Save report locally
        report_dir = f'{PROJECT_DIR}/logs'
        os.makedirs(report_dir, exist_ok=True)
        
        report_path = os.path.join(
            report_dir, 
            f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        )
        
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logging.info(f"Report saved to {report_path}")
        logging.info(f"IR Links stored in: {PROJECT_DIR}/ir_links")
        logging.info(f"Extracted reports stored in: {PROJECT_DIR}/extracted_reports")
        logging.info(f"Downloaded files stored in: {PROJECT_DIR}/downloads")
        logging.info(f"Docling parsed data stored in: {PROJECT_DIR}/data/parsed/docling")
        logging.info(f"You can manually upload these to GCP storage if needed")
        
        return report
        
    except Exception as e:
        logging.error(f"Error generating report: {str(e)}")
        return {'status': 'error', 'message': str(e)}


# Define tasks - 4 TASKS
orchestrator_task = PythonOperator(
    task_id='run_orchestrator',
    python_callable=run_orchestrator,
    dag=dag,
)

docling_task = PythonOperator(
    task_id='run_docling_parser',
    python_callable=run_docling_parser,
    dag=dag,
)

verify_task = PythonOperator(
    task_id='verify_outputs',
    python_callable=verify_outputs,
    dag=dag,
)

report_task = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
    trigger_rule='all_done',
)

# Set task dependencies - UPDATED FLOW
# orchestrator_task → docling_task → verify_task → report_task
orchestrator_task >> docling_task >> verify_task >> report_task
