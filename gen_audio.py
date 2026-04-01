import edge_tts, asyncio, os

# Dialog lines: (speaker, text)
lines = [
    ("rep",  "Thank you for calling National Auto Insurance, my name is Sarah. How can I help you today?"),
    ("cust", "Hi Sarah, my name is John Martinez. I was involved in a car accident yesterday and I need to file a claim."),
    ("rep",  "I'm sorry to hear that, Mr. Martinez. Are you okay? Was anyone injured?"),
    ("cust", "Yes, I'm fine, just a little shaken up. No injuries thankfully. But my car has significant damage to the front bumper and hood."),
    ("rep",  "I'm glad you're safe. Let me pull up your policy. Can you provide me your policy number please?"),
    ("cust", "Sure, it's P D 7 7 4 2 0 1."),
    ("rep",  "Thank you. I see your policy here. You have comprehensive coverage with a five hundred dollar deductible. Can you tell me what happened?"),
    ("cust", "I was driving on Highway 95 heading south around 3 PM yesterday. Traffic slowed down suddenly and the car in front of me stopped. I braked but couldn't stop in time and rear-ended them. It was raining pretty heavily."),
    ("rep",  "Understood. And did you get the other driver's information?"),
    ("cust", "Yes, I exchanged insurance information with the other driver. Her name is Lisa Chen and she drives a blue Toyota Camry, 2024 model."),
    ("rep",  "Perfect. And was a police report filed?"),
    ("cust", "Yes, the officers arrived about twenty minutes after the accident. The report number is 2026 dash 0330 dash 4451."),
    ("rep",  "Great, that's very helpful. I'm going to open claim number C L M 9 8 7 6 5 for you. You'll need to take your vehicle to one of our approved repair shops for an estimate. I can text you the nearest locations. Is the car still drivable?"),
    ("cust", "It's drivable but the hood doesn't close all the way. I'd rather get it looked at soon."),
    ("rep",  "Absolutely. We'll have an adjuster contact you within 24 to 48 hours to schedule an inspection. Is there anything else I can help you with today?"),
    ("cust", "No, that covers everything. Thank you so much for your help, Sarah."),
    ("rep",  "You're welcome, Mr. Martinez. We'll take good care of your claim. Have a safe rest of your day."),
]

# Voice config: female voice for Sarah (rep), male voice for John (cust)
# Rate: +25% speed for faster dialog
voice_config = {
    "rep":  {"voice": "en-US-JennyNeural",  "rate": "+25%"},   # Female
    "cust": {"voice": "en-US-GuyNeural",     "rate": "+25%"},   # Male
}

output_path = os.path.join(r"C:\Users\hiramfleitas\OneDrive - Microsoft\Documents\code\azcopy\lh1_files", "claims_call_sample.mp3")

async def generate():
    parts = []
    for i, (speaker, text) in enumerate(lines):
        cfg = voice_config[speaker]
        tmp = output_path + f".part{i}.mp3"
        communicate = edge_tts.Communicate(text, cfg["voice"], rate=cfg["rate"])
        await communicate.save(tmp)
        parts.append(tmp)
        print(f"  [{speaker.upper():4s}] {text[:60]}...")

    # Concatenate all parts into final mp3
    with open(output_path, "wb") as out_f:
        for p in parts:
            with open(p, "rb") as f:
                out_f.write(f.read())
            os.remove(p)

    print(f"\nSaved to {output_path}")
    print(f"File size: {os.path.getsize(output_path) / 1024:.1f} KB")

asyncio.run(generate())
