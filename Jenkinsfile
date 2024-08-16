#!groovy

pipeline {
    agent {
        label 'JAVA2'
    }
    options {
        buildDiscarder(logRotator(numToKeepStr: '3'))
    }
    environment {
        NEXUS_CREDENTIALS = credentials('8bed53a7-0e83-4c57-b2ae-a08d6d9eec6f')
        NEXUS_USER = "$NEXUS_CREDENTIALS_USR"
        NEXUS_PASSWORD = "$NEXUS_CREDENTIALS_PSW"
    }
    stages {
        stage('Configure') {
            steps {
                sh '''
                    echo "nexusUrl=https://nexus.geosteering.ru" > gradle.properties
                    echo "nexusUsername=$NEXUS_CREDENTIALS_USR" >> gradle.properties
                    echo "nexusPassword=$NEXUS_CREDENTIALS_PSW" >> gradle.properties
                '''
            }
        }
        stage('Compile') {
            steps {
                gradlew('clean', 'classes')
            }
        }
        stage('Tests') {
            steps {
                gradlew('check')
            }
        }
        stage('Build Docker Image') {
            steps {
                gradlew('buildDockerImage', '-PbranchName=' + env.BRANCH_NAME)
            }
        }
        stage('Push Docker Image') {
            steps {
                sh '''
                    echo $NEXUS_CREDENTIALS_PSW | docker login nexus.geosteering.ru:5001 --username $NEXUS_CREDENTIALS_USR --password-stdin
                '''

                gradlew('pushDockerImage', '-PbranchName=' + env.BRANCH_NAME)
            }
            post {
                always {
                    sh '''
                        docker logout nexus.geosteering.ru:5001
                    '''
                }
            }
        }
    }
    post {
        cleanup {
            cleanWs()
        }
    }
}

def gradlew(String... args) {
    sh "./gradlew ${args.join(' ')} -s"
}
