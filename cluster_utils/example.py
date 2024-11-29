from cluster_utils import make_farm

if __name__ == "__main__":
    make_farm(
        farm_dir = '/home/gregdeon/scratch/gregdeon/bgt-models/farms/example_farm',
        job_args = {
            'job_name': 'example_farm',
            'allocation': 'st-kevinlb-1',
            'walltime_mins': 180,
            'nodes': 1,
            'cpus': 1,
            'mem_gb': 4,
        },
        prefix = 'source /scratch/st-kevinlb-1/gregdeon/bgt-models/env/bin/activate',
        case_list = [
            'python main.py --seed 1',
            'python main.py --seed 2',
            'python main.py --seed 3',
            # etc...
        ], 
        final_args = {
            'job_name': 'example_final',
            'allocation': 'st-kevinlb-1',
            'walltime_mins': 180,
            'nodes': 1,
            'cpus': 1,
            'mem_gb': 32,
        },
        final_script = '''
source /scratch/st-kevinlb-1/gregdeon/bgt-models/env/bin/activate
python combine_data.py
'''
    )

