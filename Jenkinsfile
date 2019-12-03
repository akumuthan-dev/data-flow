pipeline {
  agent none
  parameters {
      string(name: 'GIT_COMMIT', defaultValue: 'origin/master', description: 'Commit SHA or origin branch to deploy')
  }

  stages {
    stage('prepare') {
      agent any
      steps {
        checkout([
            $class: 'GitSCM',
            branches: [[name: params.GIT_COMMIT]],
        ])
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
        ci_pipeline("dev", params.GIT_COMMIT)
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
        ci_pipeline("staging", params.GIT_COMMIT)
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
        ci_pipeline("production", params.GIT_COMMIT)
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
