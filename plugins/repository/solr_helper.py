def get_bbox(query, right_hand_envelope=False):
        # first check if there is a key: "ogc:Filter"
    if "ogc:BBOX" in query["_dict"]["ogc:Filter"]:
        lc = (
            query["_dict"]["ogc:Filter"]["ogc:BBOX"]["gml:Envelope"]["gml:lowerCorner"]
            .strip()
            .split()
        )
        uc = (
            query["_dict"]["ogc:Filter"]["ogc:BBOX"]["gml:Envelope"]["gml:upperCorner"]
            .strip()
            .split()
        )
        lc = [float(i) for i in lc]
        uc = [float(i) for i in uc]
        # Right hand polygon
        if right_hand_envelope:
            envelope = (",").join(
                [
                    f"POLYGON(({lc[0]} {lc[1]}",
                    f"{uc[0]} {lc[1]}",
                    f"{uc[0]} {uc[1]}",
                    f"{lc[0]} {uc[1]}",
                    f"{lc[0]} {lc[1]}))",
                ]
            )
            return envelope
        else:
            min_x, max_x, min_y, max_y = lc[1], uc[1], lc[0], uc[0]
            envelope = f"ENVELOPE({min_x},{max_x},{max_y},{min_y})"
            print(envelope)
            return  envelope 
    else:
        print(f"ogc:BBOX not found in {query}")
        return False