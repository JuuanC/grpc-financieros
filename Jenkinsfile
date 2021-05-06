pipeline{
    environment{
        registry = "10.30.3.239:5000/juancarlos/demo-grpc-financieros:latest"
        dockerImage=''
        containerName='demo-grpc-financieros'
    }
    agent any
    stages{
        stage('Building'){
            steps{
                sh 'mvn package -Dquarkus.package.type=legacy-jar -Dquarkus.profile=dev -DskipTests'
            }
        }

        /*stage('Testing'){
            steps{
                sh 'mvn test'
            }
        }*/

        /*stage('Quality code'){
            steps{
                sh 'mvn sonar:sonar'simple

            }
        }*/

        /*stage('Quality Gate') {
                   steps {
                        timeout(time: 1, unit: 'HOURS') {
                            Parameter indicates whether to set pipeline to UNSTABLE if Quality Gate fails
                            true = set pipeline to UNSTABLE, false = don't
                            waitForQualityGate abortPipeline: true
                  }
             }
        }*/

        stage('Docker image building'){
            steps{
                script{
                     dockerImage = docker.build(registry, " -f src/main/docker/Dockerfile.legacy-jar .")
                     //sh 'docker rmi -f $(docker images -f "dangling=true" -q)'
                }
            }           
        }

        stage('Docker pushing image'){
            steps{
                script{
                    docker.withRegistry('http://10.30.3.239:5000', 'docker-cred') {
                        dockerImage.push()
                    }
                }
            }
        }

        stage('Deploying'){
            steps{
                sshagent(credentials :['dev-server']) {
                    sh 'ssh devcrum@10.30.3.248 docker pull $registry'
                    sh 'ssh devcrum@10.30.3.248 docker rm --force $containerName'
                    sh 'ssh devcrum@10.30.3.248 docker run --name $containerName -v "/etc/timezone:/etc/timezone:ro" -v "/etc/localtime:/etc/localtime:ro" --network host -v /home/nextcloud-api/images:/home/nextcloud-api/images --restart unless-stopped -d $registry'
                }
            }
        }
    }
}