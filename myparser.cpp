#include<bits/stdc++.h>
using namespace std;
map<string,vector<string> > TABLES;
map<string,vector<vector<int> > > DATA;
vector<string>parsed_query;
vector<vector<string> > select_columns;
vector<string> select_conditions;
vector<string> tables_to_join;
vector<string> metadata_req;
vector<vector<int> >joined_data;
vector<vector<int> >final_data;
vector<pair<string,vector<int> > > final_ans;
vector<pair<string,vector<float> > > final_ans_float;

int distinct_col = 0,distinct_flag=0;
int check_error = 0;
string strip_func(string inp)
{
	inp.erase(inp.begin(), std::find_if(inp.begin(), inp.end(), std::bind1st(std::not_equal_to<char>(), ' ')));
	inp.erase(std::find_if(inp.rbegin(), inp.rend(), std::bind1st(std::not_equal_to<char>(), ' ')).base(), inp.end());
	return inp;
}
bool compare_str( const std::string& str1, const std::string& str2 ) 
{
    std::string str1Cpy( str1 );
    std::string str2Cpy( str2 );
    std::transform( str1Cpy.begin(), str1Cpy.end(), str1Cpy.begin(), ::tolower );
    std::transform( str2Cpy.begin(), str2Cpy.end(), str2Cpy.begin(), ::tolower );
    return ( str1Cpy == str2Cpy );
}
bool is_number(const std::string& s)
{
    std::string::const_iterator it = s.begin();
    if(s.find("-")!=std::string::npos)
    {
        it++;
    }
    while (it != s.end() && (std::isdigit(*it)))
    {
        ++it;
    }
    return !s.empty() && it == s.end();
}
void get_table_info()
{
    ifstream meta_file ("metadata.txt");
    if(meta_file.is_open())
    {
        string table_name;
        int table_status = 0; //0--closed 1--begin 2---table is opened
        string line;
        while(meta_file >> line)
        {
            if(line=="<begin_table>")
            {
                table_status = 1;

            }
            else if(line=="<end_table>")
                table_status = 0;
            else if(table_status==1)
            {
                vector<string> a;
                TABLES[line] = a;
                table_name = line;
                table_status = 2;
            }
            else if(table_status==2)
            {
                TABLES[table_name].push_back(line);
            }
        }
    }
    /*for(map<string, vector<string> >::const_iterator it=TABLES.begin();it!=TABLES.end();++it)
    {
        cout << it->first << endl;
        for(int i=0;i<it->second.size();i++)
            cout << it->second[i] << " ";
        cout << endl;
    }*/
    meta_file.close();
}
void store_data()
{
    for(map<string, vector<string> >::const_iterator it=TABLES.begin();it!=TABLES.end();++it)
    {
        //cout << it->first << endl;
        string table_name = it->first;
        const char *b = ".csv";
        char *a = new char[table_name.length()+1];
        strcpy(a,table_name.c_str());               //to convert char * to const char *
        ifstream data_file (strcat(a,b));        //to create file name table_name.csv
        string line;
        while(data_file >> line)
        {
            vector<int> row;
            stringstream stream_obj(line);
            string token;
            char delim = ',';
            while(getline(stream_obj,token,delim))
            {
                token.erase(remove( token.begin(), token.end(), '\"' ),token.end());
    		row.push_back(atoi(token.c_str()));
            }
            DATA[table_name].push_back(row);
        }
    }
    /*for(map<string, vector<vector<int> > >::const_iterator it=DATA.begin(); it!=DATA.end(); ++it)
    {
        cout << it->first << endl;
        for(int i=0;i<it->second.size();i++)
        {
            for(int j=0;j<it->second[i].size();j++)
            {
                cout << it->second[i][j] << " ";
            }
            cout << endl;
        }
    }*/
}
void parse_func(string query)
{
    cout << query << endl;
    char delim = ' ';
    stringstream obj(query);
    string word;
    while(getline(obj,word,delim))
    {
    	if (word.find(',')!= std::string::npos)
        {	
            stringstream obj1(word);
            string word2;
            char del=',';
            while (getline(obj1, word2, del))
            {
                parsed_query.push_back(word2);
            }
        }
	else
        {
            parsed_query.push_back(word);
	}
    }
    int where_pos = -1;
    string temp1;
    vector<string>where_vec;
    for(int i=0;i<parsed_query.size();i++)
    {
        if((compare_str(parsed_query[i],"WHERE")) || (compare_str(parsed_query[i],"where")))
        {
            where_pos = i;
            continue;
        }
        if(where_pos!=-1)
        {
            if((parsed_query[i].find("and")!=std::string::npos) || (parsed_query[i].find("AND")!=std::string::npos) || (parsed_query[i].find("or")!=std::string::npos) || (parsed_query[i].find("OR")!=std::string::npos))
            {
                if(!temp1.empty())
                {
                    //cout << "temp1 " << temp1 << endl;
                    where_vec.push_back(temp1);
                }
                where_vec.push_back(parsed_query[i]);
                temp1.clear();
            }
            else
            {
                temp1 += parsed_query[i];
            }
        }
    }
    if(where_pos==-1)
    {
        return;
    }
    if(!temp1.empty())
        where_vec.push_back(temp1);

    int vec_size = parsed_query.size();
    for(int i=where_pos+1;i<vec_size;i++)
    {
        parsed_query.erase(parsed_query.begin() + i);
    }
    for(int i=0;i<where_vec.size();i++)
    {
        parsed_query.push_back(where_vec[i]);
    }
    /*cout << parsed_query.size() << endl;
    for(int i=0;i<parsed_query.size();i++)
    {
        cout << parsed_query[i] << " ";
    }
    cout << endl;*/

}

void understand_query()
{
    int select_pos = -1, from_pos = -1, where_pos = -1;
    for(int i=0;i<parsed_query.size();i++)
    {
        if(compare_str(parsed_query[i],"SELECT"))
        {
            select_pos=i;
        }
        else if(compare_str(parsed_query[i],"FROM"))
        {
            from_pos=i;
        }
        else if(compare_str(parsed_query[i],"WHERE"))
        {
            where_pos=i;
        }
    }
    vector<string> temp;
    if(select_pos==-1)
    {
        cout << "Error" << endl;
        check_error = 1;
        return;
    }
    else
    {
        int distinct_rem = 0,check_flag = 0;
        for(int i=select_pos+1;i<from_pos;i++)
        {
            if(parsed_query[i]=="")
            {
            }
            else
            {
                vector<string> column_prop;
                parsed_query[i] = strip_func(parsed_query[i]);
                if(distinct_rem==1)
                {
                    column_prop.push_back("DISTINCT");
                    distinct_rem = 0;
                }
                else if(parsed_query[i]=="*")
                {
                    vector<string> column_prop;
                    column_prop.push_back("NONE");    // max, avg, min 
                    column_prop.push_back("NONE");    // table
                    column_prop.push_back("ALL");     // column
                    select_columns.push_back(column_prop);
                }
                else if(parsed_query[i].find("max")!=std::string::npos || parsed_query[i].find("MAX")!=std::string::npos)
                {
                    check_flag = 1;

                    column_prop.push_back("MAX");
                }
                else if(parsed_query[i].find("min")!=std::string::npos || parsed_query[i].find("MIN")!=std::string::npos)
                {
                    check_flag = 1;
                    column_prop.push_back("MIN");
                }
                else if(parsed_query[i].find("sum")!=std::string::npos || parsed_query[i].find("SUM")!=std::string::npos)
                {
                    check_flag = 1;
                    column_prop.push_back("SUM");
                }
                else if(parsed_query[i].find("distinct")!=std::string::npos || parsed_query[i].find("DISTINCT")!=std::string::npos)
                {
                    distinct_flag = 1;
                    if((parsed_query[i].find("(")!=std::string::npos)&(parsed_query[i].find(")")!=std::string::npos))
                    {
                        distinct_col = 1;
                        column_prop.push_back("DISTINCT");
                    }
                    else if((parsed_query[i].find("(")!=std::string::npos)&(parsed_query[i].find(")")==std::string::npos))
                    {
                        cout << "ERROR" << endl;
                        check_error = 1;
                        return;
                    }
                    else if((parsed_query[i].find("(")==std::string::npos)&(parsed_query[i].find(")")!=std::string::npos))
                    {
                        cout << "ERROR" << endl;
                        check_error = 1;
                        return;
                    }
                    else
                    {
                        distinct_rem = 1;
                    } 
                }
                else if(parsed_query[i].find("avg")!=std::string::npos || parsed_query[i].find("AVG")!=std::string::npos)
                {
                    check_flag = 1;
                    column_prop.push_back("AVG");
                }
                else
                {
                    column_prop.push_back("NONE");
                }
                if(column_prop.size()>0)
                {
                    if(check_flag==1)
                    {
                        check_flag = 0;
                        if((parsed_query[i].find("(")==std::string::npos)&(parsed_query[i].find(")")==std::string::npos))
                        {
                            cout << "ERROR" << endl;
                            check_error = 1;
                            return;
                        }
 
                        else if((parsed_query[i].find("(")!=std::string::npos)&(parsed_query[i].find(")")==std::string::npos))
                        {
                            cout << "ERROR" << endl;
                            check_error = 1;
                            return;
                        }
                        else if((parsed_query[i].find("(")==std::string::npos)&(parsed_query[i].find(")")!=std::string::npos))
                        {   
                            cout << "ERROR" << endl;
                            check_error = 1;
                            return;
                        }
                    }
                    string item;
                    if((column_prop[0]!="NONE"))
                    {
                        item=parsed_query[i].substr(parsed_query[i].find("(")+1,parsed_query[i].find(")")-parsed_query[i].find("(")-1);
                        item=strip_func(item);
                    }
                    else
                    {
                        item = strip_func(parsed_query[i]);
                    }
                    if(item.find(".")!=std::string::npos)
                    {
                        string col_name = item.substr(item.find(".")+1,item.size()-item.find("."));
                        string table_name = item.substr(0,item.find("."));
                        column_prop.push_back(table_name);
                        column_prop.push_back(col_name);
                    }
                    else
                    {
                        column_prop.push_back("NONE");
                    
                        column_prop.push_back(item);
                    }
                    select_columns.push_back(column_prop);
                }
                column_prop.clear();
            }
        }
    }
    /*for(int i=0;i<select_columns.size();i++)
    {
                for(int j=0;j<select_columns[i].size();j++)
                {
                        cout<<select_columns[i][j]<<" ";
                }
                printf("\n");
    }*/
    if(from_pos==-1)
    {
        printf("Error: No table to select from\n");
        check_error = 1;
        return;
    }
    else
    {
        int end=0;
        if(where_pos==-1)
        {
            end=parsed_query.size();
        }
        else
        {
            end = where_pos;
        }
        for(int i=from_pos+1;i<end;i++)
        {
            if(parsed_query[i]=="")
            {
            }
            else
            {
                parsed_query[i] = strip_func(parsed_query[i]);
                tables_to_join.push_back(parsed_query[i]);
            }
        }
    }
    /*for(int i=0;i<tables_to_join.size();i++)
    {
        cout << tables_to_join[i] << " ";
    }
    cout << endl;*/
    if(where_pos==-1)
    {
        //printf("No conditions\n");
    }
    else
    {
        //printf("Conditions to select\n");
        for(int i=where_pos+1;i<parsed_query.size();i++)
        {
            string c1,c2,c3,c4,c5,c6;
            if(parsed_query[i]=="")
            {
            }
            else
            {
                parsed_query[i] = strip_func(parsed_query[i]);
                if(parsed_query[i].find("AND")!=std::string::npos || parsed_query[i].find("and")!=std::string::npos)
                {
                    select_conditions.push_back("AND");
                }
                else if(parsed_query[i].find("OR")!=std::string::npos || parsed_query[i].find("or")!=std::string::npos)
                {
                    select_conditions.push_back("OR");
                }
                else
                {
                    string condition;
                    if(parsed_query[i].find(">=")!=std::string::npos)
                    {
                        c1=parsed_query[i].substr(0,parsed_query[i].find(">="));
                        c2=parsed_query[i].substr(parsed_query[i].find(">=")+2,parsed_query[i].size()-parsed_query[i].find(">=")+1);
                        condition = "greater_equal";
                    }
                    else if(parsed_query[i].find("<=")!=std::string::npos)
                    {
                        c1=parsed_query[i].substr(0,parsed_query[i].find("<="));
                        c2=parsed_query[i].substr(parsed_query[i].find("<=")+2,parsed_query[i].size()-parsed_query[i].find("<=")+1);
                        condition = "less_equal";
                    }
                    else if(parsed_query[i].find("!=")!=std::string::npos || parsed_query[i].find("<>")!=std::string::npos)
                    {
                        c1=parsed_query[i].substr(0,parsed_query[i].find("!="));
                        c2=parsed_query[i].substr(parsed_query[i].find("!=")+2,parsed_query[i].size()-parsed_query[i].find("!=")+1);
                        condition = "not_equal";
                    }
                    else if(parsed_query[i].find("=")!=std::string::npos)
                    {
                        c1=parsed_query[i].substr(0,parsed_query[i].find("="));
                        c2=parsed_query[i].substr(parsed_query[i].find("=")+1,parsed_query[i].size()-parsed_query[i].find("="));
                        condition = "equal";
                    }
                    else if(parsed_query[i].find(">")!=std::string::npos)
                    {
                        c1=parsed_query[i].substr(0,parsed_query[i].find(">"));
                        c2=parsed_query[i].substr(parsed_query[i].find(">")+1,parsed_query[i].size()-parsed_query[i].find(">"));
                        condition = "greater";
                    }
                    else if(parsed_query[i].find("<")!=std::string::npos)
                    {
                        c1=parsed_query[i].substr(0,parsed_query[i].find("<"));
                        c2=parsed_query[i].substr(parsed_query[i].find("<")+1,parsed_query[i].size()-parsed_query[i].find("<"));
                        condition = "lesser";
                    }
                    else
                    {
                        cout << "ERROR" << endl;
                        check_error = 1;
                        return;
                    }
                    c1 = strip_func(c1);
                    c2 = strip_func(c2);
                    if(c1.find(".")!=std::string::npos)
                    {
                        c3=c1.substr(c1.find(".")+1,c1.size()-c1.find("."));
                        c4=c1.substr(0,c1.find("."));
                        select_conditions.push_back(c4);
                        select_conditions.push_back(c3);
                    }
                    else
                    {
                        select_conditions.push_back("NONE");
                        select_conditions.push_back(c1);
                    }
                    select_conditions.push_back(condition);
                    if(c2.find(".")!=std::string::npos)
                    {
                        c3=c2.substr(c2.find(".")+1,c2.size()-c2.find("."));
                        c4=c2.substr(0,c2.find("."));
                        select_conditions.push_back(c4);
                        select_conditions.push_back(c3);
                    }
                    else
                    {
                        select_conditions.push_back("NONE");
                        select_conditions.push_back(c2);
                    }
                }
            }
        }
    }
    while(select_conditions.size()!=11)
    {
        select_conditions.push_back("NONE");
    }
    /*for(int i=0;i<select_conditions.size();i++)
    {
        cout<<select_conditions[i]<<" ";
    }
    cout<<"\n";*/
}

void Join_table_data()
{
        for(int i=0;i<tables_to_join.size();i++)
        {
                if(DATA.find(tables_to_join[i])==DATA.end())
                {
                        check_error=1;
                        printf("error\n");exit(0);
                        return;
                }
        }
        if(tables_to_join.empty())
        {
                check_error=1;
                printf("error\n");exit(0);
                return;
        }
        else
        {
                map<string,vector<vector<int> > >::iterator table_details;
                string table_name = tables_to_join[0];
                table_details = DATA.find(table_name);
                vector<vector<int> >table1_data = (*table_details).second;
                vector<int> temp3;
                for(int i=1;i<tables_to_join.size();i++)
                {
                        string table2_name=tables_to_join[i];
                        vector<vector<int> > curr_joined_data;
                        for(int j=0; j < table1_data.size(); j++)
                        {
                                for(int k=0; k < DATA[table2_name].size(); k++)
                                {
                                        vector<int> curr_row = table1_data[j];
                                        for(int l=0; l < DATA[table2_name][k].size(); l++)
                                        {
                                                curr_row.push_back(DATA[table2_name][k][l]);
                                        }
                                        curr_joined_data.push_back(curr_row);
                                        curr_row.clear();
                                }
                        }
                        table1_data = curr_joined_data;
                }
                joined_data = table1_data;
                /*for(int i=0;i<joined_data.size();i++)
                {
                        for(int j=0;j<joined_data[i].size();j++)
                        {
                                printf("%d ",joined_data[i][j]);
                        }
                        printf("\n");
                }
                int s=joined_data.size();
                printf("%d\n",s);*/
        }
}


void Join_tables()
{
    for(int i=0;i<tables_to_join.size();i++)
    {
        string table_name = tables_to_join[i];
        table_name = strip_func(table_name);
        map<string,vector<string> >::iterator it1;
        it1=TABLES.find(table_name);
        if(it1==TABLES.end()) //check if the table name is valid 
        {
            check_error=1;
            printf("error\n");
            exit(0);
            return;
        }
        else
        {
            vector<string> table_content=(*it1).second;
            for(int j=0;j<table_content.size();j++)
            {
                string table_col = table_name + "." + table_content[j];
                metadata_req.push_back(table_col); //store table.col in metadata
            }
        }
    }
    /*cout << "metadata" << endl;
    for(int i=0;i<metadata_req.size();i++)
    {
        cout<<metadata_req[i]<<" ";
    }
    printf("\n");*/
    Join_table_data();//returns joined_data where cross product of all tables is inserted
}

int compute_col(string s)
{
    int i=0;
    if(s.find(".")!=std::string::npos)
    {
        int index=-1;
        for(i=0;i<metadata_req.size();i++)
        {
            if(metadata_req[i]==s)
            {
                index=i;
                break;
            }
        }
        if(index==-1)
        {
            printf("error\n");
            exit(0);
        }
        else
        {
            return index;
        }
    }
    else
    {
        int index=-1;
        string temp;
        for(i=0;i<metadata_req.size();i++)
        {
            temp=metadata_req[i].substr(metadata_req[i].find(".")+1,metadata_req[i].size()-1-metadata_req[i].find("."));
            if(temp==s)
            {
                if(index!=-1)
                {
                    printf("error\n");
                    exit(0);
                }
                index=i;
            }                       
        }
        if(index==-1)
        {
            printf("error\n");
            exit(0);
        }
        else
        {
            return index;
        }
    }
}

vector<int> select_rows(int Case)//Case(showing is it either 1st operation or 2nd) function returns rows satisfying given conditions
{
    vector<int> final_row1;
    int col1_index,table1_index,col2_index,table2_index,oper_index;
    if(Case==1)
    {
        col1_index = 1;
        table1_index = 0;
        col2_index = 4;
        table2_index = 3;
        oper_index = 2;
    }
    else if(Case==2)
    {
        col1_index = 7;
        table1_index = 6;
        col2_index = 10;
        table2_index = 9;
        oper_index = 8;
    }
    string column1=select_conditions[col1_index];
    string table1=select_conditions[table1_index];
    string final_table1;
    if(is_number(column1))
    {
        printf("error\n");exit(0);
        check_error = 1;
        return final_row1;
    }
    else if(table1=="NONE")
    {
        final_table1=column1;
    }
    else
    {
        final_table1=table1+"."+column1;
    }
    int compare1_index = -1;
    compare1_index=compute_col(final_table1);
    if(compare1_index==-1)
    {
        check_error=1;
        printf("error\n");exit(0);
        return final_row1;
    }
    //cout << "compare1-index " << compare1_index << endl;
    int compare3=INT_MIN,compare2_index=-1;//compare3 to check if second item is a number and compare2_index-column after operator
    string column2=select_conditions[col2_index];
    string table2=select_conditions[table2_index];
    string final_table2;
    int column_flag = 1;
    if(is_number(column2))
    {
        if(table2=="NONE")
        {
            compare3=atoi(column2.c_str());
            //cout << "compare3 " << compare3 << endl;
            column_flag = 0;
        }
        else
        {
            printf("error\n");exit(0);
            check_error=1;
            return final_row1;  
        }
    }
    else if(table2=="NONE")
    {
        final_table2=column2;
    }
    else
    {
        final_table2=table2+"."+column2;
    }
    if(column_flag == 1)//2nd operator is column case
    {
        // compare values of compare1_index with values of coloumn compare2_index
        compare2_index=compute_col(final_table2);
        if(compare2_index==-1)
        {
            printf("error\n");exit(0);
            check_error=1;
            return final_row1;
        }
        //cout <<  "compare2_index " << compare2_index << endl;
    }
    // compare values of compare1_index with compare3/compare2_index values
    if(select_conditions[oper_index]=="equal")
    {
        for(int i=0;i<joined_data.size();i++)
        {
            if(((column_flag==0) &&(joined_data[i][compare1_index]==compare3)) || ((column_flag==1) &&(joined_data[i][compare1_index]==joined_data[i][compare2_index])))
            {
                final_row1.push_back(i);
            }
        }

        // below 4 lines remove redundant data 
        if(column_flag==1)
        {
            int var_flag = 0;
            if(select_columns.size()>=2)
            {
                for(int v=0;v<select_columns.size();v++)
                {
                    if(select_columns[v][2]==column1)
                        var_flag += 1;
                    else if(select_columns[v][2]==column2)
                        var_flag += 1;
                    if(var_flag>=2)
                        break;
                }
            }
            if(var_flag<2)
            {
                for(int i=0;i<joined_data.size();i++)
                {
                    joined_data[i].erase(joined_data[i].begin()+compare2_index);
                }
                metadata_req.erase(metadata_req.begin()+compare2_index);
            }
        }

    }
    else if(select_conditions[oper_index]=="not_equal")
    {
        for(int i=0;i<joined_data.size();i++)
        {
            if(((column_flag==0) && (joined_data[i][compare1_index]!=compare3)) || ((column_flag==1) &&(joined_data[i][compare1_index]!=joined_data[i][compare2_index])))
            {
                final_row1.push_back(i);
            }
        }
    }
    else if(select_conditions[oper_index]=="greater")
    {
        for(int i=0;i<joined_data.size();i++)
        {
            if(((column_flag==0) && (joined_data[i][compare1_index] > compare3)) || ((column_flag==1) &&(joined_data[i][compare1_index] > joined_data[i][compare2_index])))
            {
                final_row1.push_back(i);
            }
        }
    }
    else if(select_conditions[oper_index]=="greater_equal")
    {
        for(int i=0;i<joined_data.size();i++)
        {
            if(((column_flag==0) && (joined_data[i][compare1_index] >= compare3)) || ((column_flag==1) &&(joined_data[i][compare1_index] >= joined_data[i][compare2_index])))
            {
                final_row1.push_back(i);
            }
        }
    }
    else if(select_conditions[oper_index]=="less_equal")
    {
        for(int i=0;i<joined_data.size();i++)
        {
            if(((column_flag==0) && (joined_data[i][compare1_index] <= compare3)) || ((column_flag==1) &&(joined_data[i][compare1_index] <= joined_data[i][compare2_index])))
            {
                final_row1.push_back(i);
            }
        }
    }
    else if(select_conditions[oper_index]=="lesser")
    {
        for(int i=0;i<joined_data.size();i++)
        {
            if(((column_flag==0) && (joined_data[i][compare1_index] < compare3)) || ((column_flag==1) &&(joined_data[i][compare1_index] < joined_data[i][compare2_index])))
            {
                final_row1.push_back(i);
            }
        }
    }
    else
    {
        //cout << "wrong operator" << endl;
        printf("error\n");exit(0);
        check_error=1;
        return final_row1;
    }
    return final_row1;
}


void compute_select_data()
{
        int i=0,j=0,l=0;
        string t_name,temp1,temp2;
        vector<string> col;
        vector<int> store;
        string temp_name1,temp_name2;
        int op=0,flag1=0;
        /*for(i=0;i<select_columns.size();i++)
        {
            for(j=0;j<select_columns[i].size();j++)
                cout << select_columns[i][j] << " ";
            cout << endl;
        }*/
        for(i=0;i<select_columns.size();i++)
        {
                t_name.clear();
                col=select_columns[i];
                if(col[1]=="NONE")
                {
                        t_name=col[2];
                }
                else
                {
                        t_name=col[1];
                        t_name=t_name+"."+col[2];
                }
                if(col[0]=="MAX")
                {
                    l=compute_col(t_name);
                    t_name=metadata_req[l];

                    for(j=0;j<final_data.size();j++)
                    {
                        store.push_back(final_data[j][l]);      
                    }
                    int maxi=INT_MIN;
                    for(l=0;l<store.size();l++)
                    {
                        if(maxi<store[l])
                        {
                            maxi=store[l];
                        }
                    }
                    store.clear();
                    store.push_back(maxi);
                    t_name="max("+t_name+")";
                    final_ans.push_back(make_pair(t_name,store));
                    store.clear();
                }
                else if(col[0]=="MIN")
                {
                        l=compute_col(t_name);
                        t_name=metadata_req[l];
                        for(j=0;j<final_data.size();j++)
                        {
                                store.push_back(final_data[j][l]);      
                        }
                        int mini=INT_MAX;
                        for(l=0;l<store.size();l++)
                        {
                                if(mini>store[l])
                                {
                                        mini=store[l];
                                }
                        }
                        store.clear();
                        store.push_back(mini);
                        t_name="min("+t_name+")";
                        final_ans.push_back(make_pair(t_name,store));
                        store.clear();
                }
                else if(col[0]=="DISTINCT" || distinct_flag==1)
                {
                        if(distinct_col==0)
                        {
                                l=compute_col(t_name);
                                t_name=metadata_req[l];
                                for(j=0;j<final_data.size();j++)
                                {
                                        store.push_back(final_data[j][l]);      
                                }
                                final_ans.push_back(make_pair(t_name,store));
                                store.clear();
                        }
                        else
                        {
                                l=compute_col(t_name);
                                t_name=metadata_req[l];
                                for(j=0;j<final_data.size();j++)
                                {
                                        store.push_back(final_data[j][l]);      
                                }
                                t_name="distinct("+t_name+")";
                                final_ans.push_back(make_pair(t_name,store));
                                store.clear();
                        }
                }
                else if(col[0]=="SUM")
                {
                        l=compute_col(t_name);
                        t_name=metadata_req[l];
                        for(j=0;j<final_data.size();j++)
                        {
                                store.push_back(final_data[j][l]);      
                        }
                        int final_sum=0;
                        for(l=0;l<store.size();l++)
                        {
                                final_sum+=store[l];
                        }
                        store.clear();
                        store.push_back(final_sum);
                        t_name="sum("+t_name+")";
                        final_ans.push_back(make_pair(t_name,store));
                        store.clear();
                }
                else if(col[0]=="AVG")
                {
                        vector<float> store1;
                        l=compute_col(t_name);
                        t_name=metadata_req[l];
                        for(j=0;j<final_data.size();j++)
                        {
                                store1.push_back(final_data[j][l]);     
                        }
                        float final_sum=0;
                        for(l=0;l<store1.size();l++)
                        {
                                final_sum+=store1[l];
                        }
                        final_sum=final_sum/store1.size();
                        store1.clear();
                        store1.push_back(final_sum);
                        t_name="avg("+t_name+")";
                        final_ans_float.push_back(make_pair(t_name,store1));
                        store1.clear();
                }
                else if(col[0]=="NONE")
                {
                        if(col[2]=="ALL")
                        {
                                for(j=0;j<metadata_req.size();j++)
                                {
                                        t_name=metadata_req[j];
                                        for(l=0;l<final_data.size();l++)
                                        {
                                                store.push_back(final_data[l][j]);      
                                        }
                                        final_ans.push_back(make_pair(t_name,store));
                                        store.clear();
                                }
                        }
                        else
                        {
                                l=compute_col(t_name);
                                t_name=metadata_req[l];
                                for(j=0;j<final_data.size();j++)
                                {
                                        store.push_back(final_data[j][l]);      
                                }
                                final_ans.push_back(make_pair(t_name,store));
                                store.clear();
                        }
                }
        }
        if(!final_ans_float.empty())
        {
                vector<pair<string,vector<float> > >::iterator it1=final_ans_float.begin();
                int ptr=0;
                it1=final_ans_float.begin();
                for(i=0;i<(final_ans_float.size()-1);i++)
                {
                        cout<<final_ans_float[i].first<<",";
                }
                cout<<final_ans_float[i].first<<"\n";
                if(!final_ans_float.empty())
                {
                        for(ptr=0;ptr<final_ans_float[0].second.size();ptr++)
                        {
                                for(j=0;j<(final_ans_float.size()-1);j++)
                                {
                                        printf("%.4f,",(final_ans_float[j].second)[ptr]);
                                }
                                printf("%.4f\n",(final_ans_float[j].second)[ptr]);
                        }
                }
        }
        else if(!final_ans.empty())
        {
                vector<pair<string,vector<int> > >::iterator it1=final_ans.begin();
                int ptr=0;
                it1=final_ans.begin();
                for(i=0;i<(final_ans.size()-1);i++)
                {
                        cout<<final_ans[i].first<<",";
                }
                cout<<final_ans[i].first<<"\n";
                set<vector<int> > some_map;
                vector<int> ko;
                //      return;
                if(!final_ans.empty() && distinct_flag==0)
                {
                        for(ptr=0;ptr<final_ans[0].second.size();ptr++)
                        {
                                for(j=0;j<(final_ans.size()-1);j++)
                                {
                                        cout<<(final_ans[j].second)[ptr]<<",";
                                }
                                cout<<(final_ans[j].second)[ptr]<<"\n";
                        }
                }
                else if(!final_ans.empty() && distinct_flag==1)
                {
                        for(ptr=0;ptr<final_ans[0].second.size();ptr++)
                        {
                                ko.clear();
                                for(j=0;j<(final_ans.size());j++)
                                {
                                        ko.push_back((final_ans[j].second)[ptr]);
                                }
                                some_map.insert(ko);
                        }
                        set<vector<int> >::iterator it3;
                        it3=some_map.begin();
                        while(it3!=some_map.end())
                        {
                                for(i=0;i<(*it3).size()-1;i++)
                                {
                                        cout<<(*it3)[i]<<",";
                                }
                                cout<<(*it3)[i]<<"\n";
                                it3++;
                        }
                }
        }
}


void select_important_data()
{
    int flag1=0;
    for(int i=0;i<select_conditions.size();i++)
    {
        if(select_conditions[i]!="NONE")
        {
            flag1=1;
        }
    }
    if(flag1==0)//no where clause
    {
        final_data=joined_data;
        compute_select_data();
    }
    else
    {
        vector<int> final_rows;
        string condition;
        int and_condition=-1,or_condition=-1;
        condition = select_conditions[5]; //AND/OR/NONE
        if(condition=="AND")
        {
            vector<int> final_row1 = select_rows(1);
            if((check_error==1) && (final_row1.empty()))
            {
                check_error = 1;
                printf("error\n");exit(0);
                return;
            }
            //cout << final_row1.size() << endl;
            vector<int> final_row2 = select_rows(2);
            if((check_error==1) && (final_row2.empty()))
            {
                check_error = 1;
                printf("error\n");exit(0);
                return;
            }
            int temp_flag=0;
            for(int i=0;i<final_row1.size();i++)
            {
                temp_flag=0;
                for(int j=0;j<final_row2.size();j++)
                {
                    if(final_row1[i]==final_row2[j])
                    {
                        temp_flag=1;
                        break;
                    }
                }
                if(temp_flag==1)
                {
                    final_rows.push_back(final_row1[i]);
                }
            }
        }
        else if(condition=="OR")
        {
            vector<int> final_row1 = select_rows(1);
            if((check_error==1) && (final_row1.empty()))
            {
                check_error = 1;
                printf("error\n");exit(0);
                return;
            }
            vector<int> final_row2 = select_rows(2);
            if((check_error==1) && (final_row2.empty()))
            {
                check_error = 1;
                printf("error\n");exit(0);
                return;
            }
            set<int> temp_set;
            for(int i=0;i<final_row1.size();i++)
            {
                temp_set.insert(final_row1[i]);
            }
            for(int i=0;i<final_row2.size();i++)
            {
                temp_set.insert(final_row2[i]);
            }
            set<int>::iterator it1=temp_set.begin();
            while(it1!=temp_set.end())
            {
                final_rows.push_back((*it1));
                it1++;
            }
        }
        else
        {
            final_rows = select_rows(1);
            if((check_error==1) && (final_rows.empty()))
            {
                check_error = 1;
                printf("error\n");exit(0);
                return;
            }
        }
        // joined_data final_data final_rows
        for(int i=0;i<final_rows.size();i++)
        {
            final_data.push_back(joined_data[final_rows[i]]);
        }
        /*for(int i=0;i<final_data.size();i++)
        {
            for(int j=0;j<final_data[i].size();j++)
            {
                printf("%d ",final_data[i][j]);
            }
            printf("\n");
        }*/
        compute_select_data();
    }
}


int main(int argc,char *argv[])
{
    char *query=(char *)malloc(sizeof(char)*(strlen(argv[1])+1));
    strcpy(query,argv[1]);
    if(query[strlen(query)-1]==';')
    {
        query[strlen(query)-1]='\0';
    }
    get_table_info();//return TABLES dictionary with metadata info
    if(check_error==1)
        return 0;
    store_data();//returns DATA dictionary with each key as table name and values as rows of the table
    if(check_error==1)
        return 0;
    parse_func(query);//returns parsed_query
    if(check_error==1)
        return 0;
    understand_query();//returns select_columns(select), tables_to_join(from), select_condition(where)
    if(check_error==1)
        return 0;
    Join_tables();//returns metadata_req (list of table.col of all given columns,tables) and joined_data (list of all joined rows)
    if(check_error==1)
        return 0;
    select_important_data();
    if(check_error==1)
        return 0;

    return 0;
}
