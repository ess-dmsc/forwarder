@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

container_build_nodes = [
  'centos7': ContainerBuildNode.getDefaultContainerBuildNode('centos7-gcc8')
]

// Define number of old builds to keep.
num_artifacts_to_keep = '1'

// Set number of old builds to keep.
properties([[
  $class: 'BuildDiscarderProperty',
  strategy: [
    $class: 'LogRotator',
    artifactDaysToKeepStr: '',
    artifactNumToKeepStr: num_artifacts_to_keep,
    daysToKeepStr: '',
    numToKeepStr: ''
  ]
]]);


pipeline_builder = new PipelineBuilder(this, container_build_nodes)
pipeline_builder.activateEmailFailureNotifications()

builders = pipeline_builder.createBuilders { container ->
  pipeline_builder.stage("${container.key}: Checkout") {
    dir(pipeline_builder.project) {
      scm_vars = checkout scm
    }
    container.copyTo(pipeline_builder.project, pipeline_builder.project)
  }  // stage

  pipeline_builder.stage("${container.key}: Dependencies") {
    container.sh """
      /opt/miniconda/bin/conda init bash
      export PATH=/opt/miniconda/bin:$PATH
      python --version
      python -m pip install --user -r ${pipeline_builder.project}/requirements-dev.txt
    """
  } // stage

  pipeline_builder.stage("${container.key}: Formatting (black) ") {
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      cd ${pipeline_builder.project}
      python -m black --check .
    """
  } // stage

  pipeline_builder.stage("${container.key}: Static Analysis (flake8) ") {
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      cd ${pipeline_builder.project}
      python -m flake8
    """
  } // stage

  pipeline_builder.stage("${container.key}: Type Checking (mypy) ") {
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      cd ${pipeline_builder.project}
      python -m mypy .
    """
  } // stage

  pipeline_builder.stage("${container.key}: Test") {
    def test_output = "TestResults.xml"
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      python --version
      cd ${pipeline_builder.project}
      python -m tox -- --cov=forwarder --cov-report=xml --junitxml=${test_output}
    """
    container.copyFrom("${pipeline_builder.project}/${test_output}", ".")
    xunit thresholds: [failed(unstableThreshold: '0')], tools: [JUnit(deleteOutputFiles: true, pattern: '*.xml', skipNoTestFiles: false, stopProcessingIfError: true)]
    container.copyFrom("${pipeline_builder.project}/coverage.xml", ".")
    withCredentials([string(credentialsId: 'forwarder-codecov-token', variable: 'TOKEN')]) {
    sh "curl -s https://codecov.io/bash | bash -s - -t ${TOKEN} -C ${scm_vars.GIT_COMMIT} -f coverage.xml"
    }
  } // stage
}  // createBuilders

node {
  dir("${pipeline_builder.project}") {
    scm_vars = checkout scm
  }

  if ( env.CHANGE_ID ) {
      builders['integration tests'] = get_contract_tests_pipeline()
  }

  try {
    parallel builders
  } catch (e) {
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}

def get_contract_tests_pipeline() {
  return {
    node('docker') {
      cleanWs()
      dir("${pipeline_builder.project}") {
        try {
          stage("Contract tests: Checkout") {
            checkout scm
          }  // stage
          stage("Contract tests: Install requirements") {
            sh """
            scl enable rh-python38 -- python --version
            scl enable rh-python38 -- python -m venv test_env
            source test_env/bin/activate
            which python
            pwd
            pip install --upgrade pip
            pip install docker-compose
            """
          }  // stage
          stage("Contract tests: Prepare") {
            // Stop and remove any containers that may have been from the job before,
            // i.e. if a Jenkins job has been aborted.
            // Then pull the latest image versions
            sh """
            mkdir integration_tests/shared_volume || true
            rm -rf integration_tests/shared_volume/*
            docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true
            cd integration_tests
            grep "image:" docker-compose.yml | sed 's/image://g' | while read -r class; do docker pull \$class; done
            """
          }  // stage
          stage("Contract tests: Run") {
            timeout(time: 120, activity: true){
              sh """
              source test_env/bin/activate
              cd integration_tests
              docker-compose up &
              sleep 60
              rsync -av .. shared_volume/forwarder --exclude=shared_volume --exclude=".*"
              docker exec integration_tests_bash_1 bash -c 'cd shared_source/forwarder/integration_tests/contract_tests; pytest --junitxml=ContractTestsOutput.xml'
              cp shared_volume/forwarder/integration_tests/contract_tests/ContractTestsOutput.xml .
              """
            }
          }  // stage
        } finally {
          stage ("Contract tests: Clean Up") {
            // The statements below return true because cleaning up should
            // not affect the results of the tests.
            sh """
            source test_env/bin/activate
            docker-compose down || true
            rm -rf test_env || true
            rm -rf integration_tests/shared_source/* || true
            docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true
            """
          }  // stage
          stage("Contract tests: Archive") {
            junit "integration_tests/ContractTestsOutput.xml"
          }
        }  // try/finally
      } // dir
    }  // node
  }  // return
} // def
