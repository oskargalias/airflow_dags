from airflow import DAG
from airflow.operators.python_operator import PythonVirtualenvOperator
import datetime
import os
from airflow.models import Variable

main_config = dict(
    repo_full_name = 'oskargalias/standard_server_jupyter',
    py_file_to_run = 'VK2/scripts/daily.py',
    req_path = "VK2/req",
)
        
def gitrun(
    login, token,
    repo_full_name,
    py_file_to_run,
    req_path
):
    # from github import Github, Auth
    import os, getpass

    username = getpass.getuser()
    repo_name = repo_full_name.split('/')[-1]
    req_path = req_path if '.' in req_path else req_path+'.txt' # это из-за странной ошибки парсинга, если сразу в пути дать .txt файл
    
    os.system('apt-get update')
    os.system('apt-get -y install git')
        
    os.system('rm -rf '+repo_name) # remove folder if exists (we need to update repo)
    os.system(f"git clone https://{login}:{token}@github.com/{repo_full_name}.git")
    print('repo cloned.')

    
    # print('getcwd =', os.getcwd())
    # print('listdir =', os.listdir(os.getcwd()))
    
    if os.path.exists(os.path.join(os.getcwd(), py_file_to_run)):
        path = os.path.join(os.getcwd(), py_file_to_run)
    elif os.path.exists(os.path.join(os.getcwd(), repo_name, py_file_to_run)):
        path = os.path.join(os.getcwd(), repo_name, py_file_to_run)
    else:
        raise Exception(f'{py_file_to_run} was not found in path {os.path.join(os.getcwd(), repo_name)}')
    
    if os.path.exists(os.path.join(os.getcwd(), req_path)):
        req_full_path = os.path.join(os.getcwd(), req_path)
    elif os.path.exists(os.path.join(os.getcwd(), repo_name, req_path)):
        req_full_path = os.path.join(os.getcwd(), repo_name, req_path)
    else:
        raise Exception(f'{req_path} was not found in path {os.path.join(os.getcwd(), repo_name)}')
    
    ## run file
    import subprocess
    # load requirements
    print(f'Try to loaf reqs {req_full_path}')
    print(subprocess.getoutput(f"sudo -H -u {username} bash -c 'pip install -r {req_full_path}'"))
    # run script
    print(f'Try to run {path}')
    print(subprocess.getoutput("python "+path))
    print('Seems to be success!')
    
    
dag = DAG(
    'vk_daily', 
    schedule="27 10 * * *",
    start_date=datetime.datetime(2024, 9, 17, 0),
    tags=["gitrun"]
    )

function = PythonVirtualenvOperator( 
    task_id = 'gitrun',
    python_callable = gitrun,
    dag=dag,
    op_kwargs={
        'login': Variable.get("github_login", default_var = None),
        'token': Variable.get("github_token", default_var = None),
        'repo_full_name': main_config.get('repo_full_name'),
        'py_file_to_run': main_config.get('py_file_to_run'), 
        'req_path': main_config.get("req_path"),
              },
    
    # requirements=["PyGithub==2.4.0", "vk_api==11.9.9", "numpy==1.26.4", "pandas==2.1.4"],
    # python_version='3.10',
    # system_site_packages=False,
)

function
