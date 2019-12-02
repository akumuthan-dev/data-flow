pipeline {
  agent none
  parameters {
      string(name: 'VERSION', defaultValue: 'master', description: 'Branch name or commit SHA to deploy')
  }

  stages {
    stage('prepare') {
      agent any
      steps {
        checkout scm
        script {
          pullRequestNumber = sh(
              script: "git log -1 --pretty=%B | grep 'Merge pull request' | cut -d ' ' -f 4 | tr -cd '[[:digit:]]'",
              returnStdout: true
          ).trim()
          currentBuild.displayName = "#${env.BUILD_ID} - PR #${pullRequestNumber}"
        }
      }
    }


    stage('release: dev') {
      steps {
        ci_pipeline("dev", params.VERSION)
      }
    }


    stage('release: staging') {
      when {
          expression {
              milestone label: "release-staging"
              input message: 'Deploy to staging?'
              return true
          }
          beforeAgent true
      }

      steps {
        ci_pipeline("staging", params.VERSION)
      }
    }


    stage('release: prod') {
      when {
          expression {
              milestone label: "release-prod"
              input message: 'Deploy to prod?'
              return true
          }
          beforeAgent true
      }

      steps {
        ci_pipeline("production", params.VERSION)
      }
    }

  }
}

void ci_pipeline(env, version) {
  build job: "ci-pipeline", parameters: [
    string(name: "Team", value: "datasci"),
    string(name: "Project", value: "data-flow"),
    string(name: "Environment", value: env),
    string(name: "Version", value: version)
  ]
}
