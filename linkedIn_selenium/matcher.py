from thefuzz import fuzz, process
from typing import Literal, List

def fuzz_match(ls1:List[str],ls2:List[str], method:Literal["Qratio","Wratio","normal","partial"] = "partial"):
    if len(ls1) == 0 or len(ls2) == 0:
        return None
    elif method == "partial":
        return fuzz.partial_token_sort_ratio(ls1,ls2)
    elif method == "Qratio":
        return fuzz.QRatio(ls1,ls2)
    elif method == "Wratio":
        return fuzz.WRatio(ls1,ls2)
    elif method == "normal":
        return fuzz.token_sort_ratio(ls1,ls2)

def find_matches(ls1, ls2, threshold=80):
    matches = []
    for keyword in ls1:
        # Find the best match in list2 for each keyword in list1
        best_match = process.extractOne(keyword, ls2)
        if best_match and best_match[1] >= threshold:
            matches.append(keyword)
    if len(matches) == 0:
        return None
    return matches

def produce_match_columns(job_skills: List[str], my_skills: List[str], threshold: int=70):
    return {
        "match_score": fuzz_match(job_skills,my_skills,method='partial'),
        "top_matches": find_matches(job_skills,my_skills,threshold),
        "match_threshold": threshold
    }

if __name__ == "__main__":
    # my_skills = dotenv.get_key(".env","MY_SKILLS")
    my_skills = ["python","javascript","angular","back-end","flask","front-end","aws","gcp"]
    their_skills = ["python language","ecmascript","front-end"]
    res = find_matches(my_skills,their_skills,70)
    print(res)