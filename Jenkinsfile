pipeline {

    agent any

    stages {

        stage('Project Information') {
            steps {
                echo 'E-Commerce ETL Pipeline'
            }
        }

        stage('Copy Project') {
            steps {
                sh '''
                cp -r /project/. .
                ls -la
                '''
            }
        }

        stage('Check Reports') {
            steps {
                sh 'ls -la reports'
            }
        }

        stage('Check Dashboard') {
            steps {
                sh 'test -f reports/dashboard.html'
                sh 'echo Dashboard found'
            }
        }

        stage('Check CSV') {
            steps {
                sh 'test -f reports/kpi_report.csv'
                sh 'echo KPI report found'
            }
        }

        stage('Finished') {
            steps {
                echo 'Project completed successfully.'
            }
        }
    }
}