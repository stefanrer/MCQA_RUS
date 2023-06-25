import json
import os
import spacy
import h5py
 

def compare_sentences(docs):
    if os.path.exists(os.path.join("result", "result.pickle")):
        result = pickle.load(open(os.path.join("result", "result.pickle"), "rb"))
        start_idx = pickle.load(open(os.path.join("result", "start_idx.pickle"), "rb"))
        skip_idx_set = pickle.load(open(os.path.join("result", "skip_idx_set.pickle"), "rb"))
        print(f"Start from: {start_idx}")
    else:
        start_idx = 0
        result = []
        skip_idx_set = set()
        
    if start_idx == len(docs):
        print("Alreadt exist")
        return result
    for idx_doc1, doc1 in enumerate(docs[start_idx:]):
        if (start_idx + idx_doc1) < 1000:
            if (start_idx + idx_doc1) % 10 == 0:
                picle_save(result, start_idx + idx_doc1, skip_idx_set) 
        elif (start_idx + idx_doc1) < 10000:
            if (start_idx + idx_doc1) % 50 == 0:
                picle_save(result, start_idx + idx_doc1, skip_idx_set)
        elif (start_idx + idx_doc1) < 100000:
            if (start_idx + idx_doc1) % 100 == 0:
                picle_save(result, start_idx + idx_doc1, skip_idx_set)
        elif (start_idx + idx_doc1) < 1000000:
            if (start_idx + idx_doc1) % 500 == 0:
                picle_save(result, start_idx + idx_doc1, skip_idx_set)
        else:
            if (start_idx + idx_doc1) % 1000 == 0:
                picle_save(result, start_idx + idx_doc1, skip_idx_set)
                
        if (start_idx + idx_doc1) % 1000 == 0:
            print(f"Progress: {start_idx + idx_doc1} / {len(docs)}")
            
        if  start_idx + idx_doc1 in skip_idx_set:
            continue
            
        if doc1[5].vector_norm:
            for idx_doc2, doc2 in enumerate(docs[start_idx + idx_doc1+1:]):
                if doc2[5].vector_norm:
                    if start_idx + idx_doc1 + 1 + idx_doc2 not in skip_idx_set:
                        if doc1[5].similarity(doc2[5]) > 0.95:
                            skip_idx_set.add(start_idx + idx_doc1 + 1 + idx_doc2)
        result.append([docs[idx_doc1][0], docs[idx_doc1][1], docs[idx_doc1][2],docs[idx_doc1][3],docs[idx_doc1][4],docs[idx_doc1][5].text])
    pickle.dump(result, open(os.path.join("result", "result.pickle"), "wb"))
    pickle.dump(len(docs), open(os.path.join("result", "start_idx.pickle"), "wb"))
    pickle.dump(skip_idx_set, open(os.path.join("result", "skip_idx_set.pickle"), "wb"))
    return result


def load_data_from_file(jfile_content, filename):
    nlp_compare = spacy.load('ru_core_news_md')
    pickle.dump([[sent[0], sent[1], sent[2], sent[3], sent[4], nlp_compare(sent[5])] for sent in jfile_content], open(os.path.join("docs", filename + "_doc.pickle"), "wb"))
    #nlp_compare_data = [[sent[0], sent[1], sent[2], sent[3], sent[4], nlp_compare(sent[5])] for sent in jfile_content]
    pickle.dump(nlp_compare, open(os.path.join("nlp", filename + "_nlp.pickle"), "wb"))
    #pickle.dump(nlp_compare_data, open(os.path.join("docs", filename + "_doc.pickle"), "wb"))
    #return nlp_compare_data


# Create spacy embedings
data_set = []
size_counter = 0
for root, dirs, files in os.walk('Dataset'):
    for file in files:
        filepath = os.path.join(root, file)
        with open(filepath, "r", encoding="UTF-8") as infile:
            #exist, nlp, data = check_if_exist(os.path.splitext(file)[0])
            #if exist:
                #data_set += data
             #   pass
            #else:
                #data_set += load_data_from_file([[os.path.splitext(file)[0], op[0], op[1], op[2], op[3], op[4]] for op in list(json.load(infile).values())[0]], os.path.splitext(file)[0])
                load_data_from_file([[os.path.splitext(file)[0], op[0], op[1], op[2], op[3], op[4]] for op in list(json.load(infile).values())[0]], os.path.splitext(file)[0])
            #print(f"Created/Loaded NLP for {os.path.splitext(file)[0]} | Size: {len(data_set)-size_counter}")
            #size_counter = len(data_set)
            
print(f"Created All NLP DOCS | Total size: {len(data_set)}")


# Clean data_set
#print(f"Start Cleaning...")
#data_set = compare_sentences(data_set)
#print(f"Finished Cleaning, result size: {len(data_set)}")
# Create Dataset json
#json_object = {"final" : data_set}
#with open(os.path.join("result", "result.json"), 'w', encoding='utf8') as json_file:
    #json.dump(json_object, json_file, ensure_ascii=False)