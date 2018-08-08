from libsubmit.providers import SlurmProvider
from libsubmit.channels import LocalChannel
from libsubmit.launchers import SrunLauncher

from parsl.config import Config
from parsl.executors.threads import ThreadPoolExecutor
from parsl.executors.ipp import IPyParallelExecutor
from parsl.executors.ipp_controller import Controller

cori_debug_config = Config(
    executors=[
        IPyParallelExecutor(
            label='ipp_slurm',
            provider=SlurmProvider(
                'debug',
                nodes_per_block=1,
                tasks_per_node=64,
                init_blocks=1,
                max_blocks=1,
                walltime="00:30:00",
                overrides="#SBATCH --constraint=haswell"
            )
        )
    ]
)

cori_regular_config = Config(
    executors=[
        IPyParallelExecutor(
            label='ipp_slurm',
            provider=SlurmProvider(
                'regular',
                nodes_per_block=1,
                tasks_per_node=64,
                init_blocks=1,
                max_blocks=4,
                parallelism=0.5,
                walltime="00:24:00",
                overrides="#SBATCH --constraint=haswell"
            )
        )
    ]
)


threads_config =  Config(
    executors=[ThreadPoolExecutor(
            max_threads=8,
            label='local_threads'
            )],
    lazy_errors=True
)
