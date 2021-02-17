@Library('ecdc-pipeline')
import ecdcpipeline.ContainerBuildNode
import ecdcpipeline.PipelineBuilder

project = "forwarder"

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
    numToKeepStr: num_artifacts_to_keep
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
    def conan_remote = "ess-dmsc-local"
    container.sh """
      /opt/miniconda/bin/conda init bash
      export PATH=/opt/miniconda/bin:$PATH
      python --version
      python -m pip install --user -r ${project}/requirements-dev.txt
    """
  } // stage

  pipeline_builder.stage("${container.key}: Formatting (black) ") {
    def conan_remote = "ess-dmsc-local"
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      cd ${project}
      python -m black --check .
    """
  } // stage

  pipeline_builder.stage("${container.key}: Static Analysis (flake8) ") {
    def conan_remote = "ess-dmsc-local"
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      cd ${project}
      python -m flake8
    """
  } // stage

  pipeline_builder.stage("${container.key}: Type Checking (mypy) ") {
    def conan_remote = "ess-dmsc-local"
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      cd ${project}
      python -m mypy .
    """
  } // stage

  pipeline_builder.stage("${container.key}: Test") {
    def test_output = "TestResults.xml"
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      python --version
      cd ${project}
      python -m pytest --cov=forwarder --cov-report=xml --junitxml=${test_output}
    """
    container.copyFrom("${project}/${test_output}", ".")
    xunit thresholds: [failed(unstableThreshold: '0')], tools: [JUnit(deleteOutputFiles: true, pattern: '*.xml', skipNoTestFiles: false, stopProcessingIfError: true)]
    container.copyFrom("${project}/coverage.xml", ".")
    withCredentials([string(credentialsId: 'forwarder-codecov-token', variable: 'TOKEN')]) {
    sh "curl -s https://codecov.io/bash | bash -s - -t ${TOKEN} -C ${scm_vars.GIT_COMMIT} -f coverage.xml"
    }
  } // stage
}  // createBuilders

node {
  dir("${project}") {
    scm_vars = checkout scm
  }

  if ( env.CHANGE_ID ) {
      builders['system tests'] = get_system_tests_pipeline()
  }

  try {
    parallel builders
  } catch (e) {
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}

def get_system_tests_pipeline() {
  return {
    node('system-test') {
      cleanWs()
      dir("${project}") {
        try {
          stage("System tests: Checkout") {
            checkout scm
          }  // stage
          stage("System tests: Install requirements") {
            sh """
            export PATH=/opt/miniconda/bin
            python --version
            python -m venv test_env
            source test_env/bin/activate
            which python
            pwd
            pip install --upgrade pip
            pip install -r requirements-dev.txt
            pip install -r system_tests/requirements.txt
            """
          }  // stage
          stage("System tests: Run") {
            // Stop and remove any containers that may have been from the job before,
            // i.e. if a Jenkins job has been aborted.
            sh "docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true"
            timeout(time: 30, activity: true){
              sh """
              source test_env/bin/activate
              cd system_tests/
              python -m pytest -s --junitxml=./SystemTestsOutput.xml .
              """
            }
          }  // stage
        } finally {
          stage ("System tests: Clean Up") {
            // The statements below return true because the build should pass
            // even if there are no docker containers or output files to be
            // removed.
            sh """
            rm -rf test_env
            rm -rf system_tests/output-files/* || true
            docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true
            """
          }  // stage
          stage("System tests: Archive") {
            junit "system_tests/SystemTestsOutput.xml"
          }
        }  // try/finally
      } // dir
    }  // node
  }  // return
} // def
