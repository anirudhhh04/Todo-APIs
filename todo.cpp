#include<iostream>
#include<sstream>
#include<arpa/inet.h>
#include<unistd.h>
#include<unordered_map>
#include<string>
#include<fstream>
#include<iomanip>
#include<thread>
#include<mutex>
#include<atomic>
#include <queue>
#include <condition_variable>
#include <functional>
#include <vector>
#include<signal.h>
#include "json.hpp" //library for json parsing
#include <fcntl.h>
#include <chrono>
#include <mysql/mysql.h> 
MYSQL*conn;
using json=nlohmann::json;
#define PORT 5000
std::queue<int> taskQueue;
std::mutex queueMutex; //mutex for queue accessing
std::condition_variable cv;
std::atomic<bool>running{true}; //for graceful shutdown of threads
struct todo{
    int id;
    std::string title;
    std::string status;

};
struct Request {
    std::string raw;
    std::string method;
    std::string path;
    std::string routePath;
    std::string body;
    std::unordered_map<std::string, std::string> headers;
    std::chrono::steady_clock::time_point startTime;
};

struct Response {
    int statusCode = 200;
    std::string statusText = "OK";
    std::string contentType = "application/json";
    std::string body;
    std::string toHttpString() {
        std::string response;
        response="HTTP/1.1 "+std::to_string(statusCode)+" "+statusText+"\r\n";
        response+="Content-Type: "+contentType + "\r\n";
        response+="Content-Length: "+std::to_string(body.size())+"\r\n";
        response+="\r\n";
        response+=body;
        return response;
    }
};
void signalHandler(int signum){ //for graceful shut down
    running=false;
    cv.notify_all();
}
using Handler=std::function<void(Request&,Response&)>; //handler type for routing
using Middleware=std::function<bool(Request&,Response&)>; //middleware type
std::unordered_map<std::string,std::unordered_map<std::string,Handler>> router; //used to map method to handler function
std::vector<Middleware>middlewares;
//std::unordered_map<int,todo> T; //for storage
//std::atomic<int> id{1}; //prevents race condition
//std::mutex dataMutex; //mutex for finding and changing the data
std::mutex logMutex; //mutex for logging
//for exctracting method and path
void parseLine(const std::string &request, std::string &method, std::string &path){
    std::stringstream ss(request);
    ss >> method >> path;
}
//for getting id from query
int getId(const std::string &path){
    size_t p=path.find("id=");
    if(p!=std::string::npos){
        return std::stoi(path.substr(p + 3));
    }
    return -1;
}
void parseHeaders(const std::string& headersText, Request& req){
    std::stringstream ss(headersText);
    std::string line;
    //first request line is already handled by parseLine()
    std::getline(ss,line);
    while(std::getline(ss,line)){
        if(!line.empty() && line.back() == '\r'){  //removes \r
            line.pop_back();
        }
        size_t pos=line.find(":");
        if(pos != std::string::npos){
            std::string key=line.substr(0, pos);
            std::string value=line.substr(pos + 1);

            if(!value.empty() && value[0] == ' '){
                value.erase(0, 1);
            }
            req.headers[key] = value;
        }
    }
}
//for logging each api call
void logRequest(const std::string&method,const std::string&path,const std::string&status){
    std::lock_guard<std::mutex> lock(logMutex);
    std::ofstream logFile("server.log",std::ios::app);
    auto now=std::chrono::system_clock::now(); //curent time
    auto time=std::chrono::system_clock::to_time_t(now); //formatting  
    logFile<<std::
    put_time(std::localtime(&time), "%Y-%m-%d %H:%M:%S")<<" | "<<method<<" | " <<path<< " | " <<status << "\n";
}
bool loggingMiddleware(Request&req,Response&res){ //middleware for logging
    req.startTime=std::chrono::steady_clock::now();
    std::cout <<"\n[LOG] Incoming Request:\n";
    std::cout <<req.method << " "<< req.path << "\n";
    return true;
}
void logCompletedRequest(Request&req, Response&res){ //for centralized logging
    auto endTime=std::chrono::steady_clock::now();
    auto latency=std::chrono::duration_cast<std::chrono::milliseconds>(endTime-req.startTime).count();
    std::string status=std::to_string(res.statusCode)+" "+res.statusText +" | " + std::to_string(latency) + "ms";
    std::cout<<"[LOG] Completed: "<< req.method << " "<< req.path << " | "<< status << "\n";
    logRequest(req.method, req.path, status);
}
bool authMiddleware(Request&req,Response&res){ //middleware for authentication
    const std::string validToken="mysecretToken";
    if(req.headers.find("Authorization")==req.headers.end() || req.headers["Authorization"]!= "Bearer "+validToken){ //checks valid header
        json er;
        er["error"]="Forbidden";
        res.statusCode=403;
        res.statusText="Forbidden";
        res.body=er.dump();
        return false;
    }
    return true;
}
void handleCreate(Request&req,Response&res){
            std::string title,status;
            try{
             json j=json::parse(req.body);
             title=j["title"];
             status=j["status"];
            }
            catch(const std::exception &e){
                std::string resBody=R"({"error":"invalid JSON"})";
                res.statusCode=400;
                res.statusText="Bad Request";
                res.body=resBody;
                return;
            }
            std::string query="INSERT INTO todo(title,status) VALUES('"+ title +"','" +status +"')";
            if(mysql_query(conn,query.c_str())!=0){
                 json er;
                 er["error"]="Database Error";
                 res.statusCode=500;
                 res.statusText="Internal Server Error";
                 res.body=er.dump();
                 return;
            }
            json j;
            j["message"]="Todo Created";
            std::string resBody=j.dump();
            res.statusCode=201;
            res.statusText="Created";
            res.body=resBody;
}
void handleGet(Request&req,Response&res){
            int idd=getId(req.path);
            std::string query="SELECT id,title,status FROM todo WHERE id="+std::to_string(idd);
            if(mysql_query(conn,query.c_str())!=0){
               json er;
               er["error"]=mysql_error(conn);
               res.statusCode=500;
               res.statusText="Internal Server Error";
               res.body=er.dump();
               return;
            }
            MYSQL_RES* result=mysql_store_result(conn);
            MYSQL_ROW row=mysql_fetch_row(result);
            if(row){
                json j;
                j["id"]=std::stoi(row[0]);
                j["title"]=row[1];
                j["status"]=row[2];
                res.statusCode=200;
                res.statusText="OK";
                res.body=j.dump();
            }
            else{
                json er;
                er["error"]="Not Found";
                res.statusCode=404;
                res.statusText="Not Found";
                res.body=er.dump();
            }
            mysql_free_result(result);
}
void handleUpdate(Request& req, Response& res){
            std::string title, status;
            int idd;
            // Parse JSON
            try{
                 json j = json::parse(req.body);
                 idd = j["id"];
                 title = j["title"];
                 status = j["status"];
            }
            catch(const std::exception& e){
                json er;
                er["error"] = "Invalid JSON";
                res.statusCode = 400;
                res.statusText = "Bad Request";
                res.body = er.dump();
                return;
            }
            std::string query="UPDATE todo SET title='" +title +"', status='" +status +"' WHERE id=" +std::to_string(idd);
            // Execute query
            if(mysql_query(conn, query.c_str()) != 0){
                json er;
                er["error"] = mysql_error(conn);
                res.statusCode = 500;
                res.statusText = "Internal Server Error";
                res.body = er.dump();
                return;
            }
            // Check if any row was updated
            my_ulonglong rows = mysql_affected_rows(conn);
            if(rows>0){
                json j;
                j["message"] = "Updated";
                res.statusCode = 200;
                res.statusText = "OK";
                res.body = j.dump();
            }
            else{
               json er;
               er["error"] = "Not Found";
               res.statusCode = 404;
               res.statusText = "Not Found";
               res.body = er.dump();
            }
}
void handleDelete(Request& req, Response& res){
            int idd = getId(req.path);
            std::string query ="DELETE FROM todo WHERE id=" +std::to_string(idd);
            if(mysql_query(conn, query.c_str()) != 0){
               json er;
               er["error"] = mysql_error(conn);
               res.statusCode = 500;
               res.statusText = "Internal Server Error";
               res.body = er.dump();
               return;
            }
            my_ulonglong rows = mysql_affected_rows(conn);
            if(rows>0){
               json j;
               j["message"] = "Deleted";
               res.statusCode = 200;
               res.statusText = "OK";
               res.body = j.dump();
            }
            else{
               json er;
               er["error"] = "Not Found";
               res.statusCode = 404;
               res.statusText = "Not Found";
               res.body = er.dump();
            }
}
void handleClient(int client_fd){
        std::string rawRequest;
        char buffer[1024];
        int bytes;
        //read until headers complete
        while ((bytes=read(client_fd,buffer,sizeof(buffer)))>0) {
               rawRequest.append(buffer,bytes);
               if (rawRequest.find("\r\n\r\n")!=std::string::npos) break;
        }
        Request req;
        Response res;
        req.raw=rawRequest;
        std::cout << "\n Request:\n"<<rawRequest<<"\n";
        parseLine(req.raw,req.method,req.path);
        req.routePath=req.path; //for handling in router map without id params
        size_t q=req.path.find('?');
        if (q != std::string::npos) {
             req.routePath=req.path.substr(0,q);
        }
        size_t headerEnd=rawRequest.find("\r\n\r\n"); //split header
        if(headerEnd==std::string::npos){
            json er;
            er["error"]="Bad Request";
            std::string response;
            res.statusCode=400;
            res.statusText="Bad Request";
            res.body=er.dump();
            response=res.toHttpString();
            send(client_fd,response.c_str(),response.size(),0);
            close(client_fd);
            return;
        }
        std::string headers=rawRequest.substr(0,headerEnd);
        req.body=rawRequest.substr(headerEnd+4);
        parseHeaders(headers,req);
        int l=0;
        try {
            if(req.headers.find("Content-Length") != req.headers.end()){
              l = std::stoi(req.headers["Content-Length"]);
           }
        }
        catch(...){
            json er;
            er["error"] = "Invalid Content-Length";
            res.statusCode = 400;
            res.statusText = "Bad Request";
            res.body = er.dump();
            std::string response = res.toHttpString();
            send(client_fd,response.c_str(),response.size(),0);
            close(client_fd);
            return;
        }
        //read remaining body
        while(req.body.size()< l){
            bytes=read(client_fd,buffer,sizeof(buffer));
            if(bytes<=0) break;
            req.body.append(buffer,bytes);
        }
        //executing middlewares before routing
        for(auto &m:middlewares){
            if(!m(req,res)){
                logCompletedRequest(req,res);
                std::string response=res.toHttpString();
                send(client_fd,response.c_str(),response.size(),0);
                close(client_fd);
                return;
            }
        }
        if(router.count(req.method) && router[req.method].count(req.routePath)){ //checks if handler exists in router map
                router[req.method][req.routePath](req,res);}
        else{
            json er;
            er["error"]="Not Found";
            res.statusCode=404;
            res.statusText="Not Found";
            res.body=er.dump();
        }
        logCompletedRequest(req,res);
        std::string response=res.toHttpString();
        send(client_fd,response.c_str(),response.size(),0);
        close(client_fd);

}
//worker thread which waits for tasks from queue
void worker(){
    while(running){
        int client;
        //critical section for accessing client from task queue
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            cv.wait(lock,[]{ return !taskQueue.empty() || !running; }); //wait until queue has a client
            if(!running && taskQueue.empty()) return;
            client=taskQueue.front(); //take client from queue
            taskQueue.pop();
        }
        handleClient(client); //process client
    }
}
int main(){
    int sockfd,client_fd;
    struct sockaddr_in server;
    int l=sizeof(server);
    sockfd=socket(AF_INET,SOCK_STREAM,0);
    fcntl(sockfd, F_SETFL, O_NONBLOCK); //make socket non blocking so accept() doesnt block forever
    server.sin_family=AF_INET;
    server.sin_port=htons(PORT);
    server.sin_addr.s_addr=INADDR_ANY;
    if(bind(sockfd,(struct sockaddr*)&server,sizeof(server))<0){
        perror("Binding Failed");
        return 1;
    }
    listen(sockfd,5);
    conn=mysql_init(NULL);
    if(!mysql_real_connect(conn,"172.24.192.1","root","12345","todo_db",3306,NULL,0)){
        std::cerr<<"Database connection failed: "<< mysql_error(conn)<< "\n";
        return 1;
    }
    std::cout <<"server running on http://localhost:5000\n";
    router["POST"]["/create"]=handleCreate;
    router["GET"]["/get"]=handleGet;
    router["PUT"]["/update"]=handleUpdate;
    router["DELETE"]["/delete"]=handleDelete;
    middlewares.push_back(loggingMiddleware);
    middlewares.push_back(authMiddleware);
    std::vector<std::thread> workers; //create thread pool with 4 workers
    for(int i=0;i<4;i++){
        workers.emplace_back(worker);
    }
    signal(SIGINT,signalHandler);
    while(running){
        client_fd=accept(sockfd,(struct sockaddr*)&server,(socklen_t*)&l);
        if(client_fd<0){
             std::this_thread::sleep_for(std::chrono::milliseconds(10)); // small delay to avoid continuous busy polling
             continue;
        }
        {
          std::lock_guard<std::mutex> lock(queueMutex);
          taskQueue.push(client_fd);//push client to task queue
        }
        cv.notify_one(); //notify one worker thread
    }
    cv.notify_all();
    for(auto &t: workers){
        t.join(); //main thread stops only after executing all other threads
    }
    mysql_close(conn);
    return 0;
}