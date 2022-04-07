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
    def conan_remote = "ess-dmsc-local"
    container.sh """
      /opt/miniconda/bin/conda init bash
      export PATH=/opt/miniconda/bin:$PATH
      python --version
      python -m pip install --user -r ${pipeline_builder.project}/requirements-dev.txt
    """
  } // stage

  pipeline_builder.stage("${container.key}: Formatting (black) ") {
    def conan_remote = "ess-dmsc-local"
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      cd ${pipeline_builder.project}
      python -m black --check .
    """
  } // stage

  pipeline_builder.stage("${container.key}: Static Analysis (flake8) ") {
    def conan_remote = "ess-dmsc-local"
    container.sh """
      export PATH=/opt/miniconda/bin:$PATH
      cd ${pipeline_builder.project}
      python -m flake8
    """
  } // stage

  pipeline_builder.stage("${container.key}: Type Checking (mypy) ") {
    def conan_remote = "ess-dmsc-local"
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
      builders['integration tests'] = get_integration_tests_pipeline()
  }

  try {
    parallel builders
  } catch (e) {
    throw e
  }

  // Delete workspace when build is done
  cleanWs()
}

def get_integration_tests_pipeline() {
  return {
    node('system-test') {
      cleanWs()
      dir("${pipeline_builder.project}") {
        try {
          stage("Integration tests: Checkout") {
            checkout scm
          }  // stage
          stage("Integration tests: Install requirements") {
            sh """
            python3 --version
            python3 -m venv test_env
            source test_env/bin/activate
            which python3
            pwd
            pip3 install --upgrade pip
            pip3 install -r requirements-dev.txt
            pip3 install -r integration_tests/requirements.txt
            """
          }  // stage
          stage("Integration tests: Run") {
            // Stop and remove any containers that may have been from the job before,
            // i.e. if a Jenkins job has been aborted.
            sh "docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true"
            timeout(time: 30, activity: true){
              sh """
              source test_env/bin/activate
              cd integration_tests/
              python3 -m pytest -s --junitxml=./IntegrationTestsOutput.xml .
              """
            }
          }  // stage
        } finally {
          stage ("Integration tests: Clean Up") {
            // The statements below return true because the build should pass
            // even if there are no docker containers or output files to be
            // removed.
            sh """
            rm -rf test_env
            rm -rf integration_tests/output-files/* || true
            docker stop \$(docker ps -a -q) && docker rm \$(docker ps -a -q) || true
            """
          }  // stage
          stage("Integration tests: Archive") {
            junit "integration_tests/IntegrationTestsOutput.xml"
          }
        }  // try/finally
      } // dir
    }  // node
  }  // return
} // def
