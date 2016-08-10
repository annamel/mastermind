find . -type f -iname \*.py -print0 | tar czvf ../package.tar.gz --null -T - &&\
#    cocaine-tool app upload --package ../package.tar.gz -n mastermind2.26-inventory --manifest /home/indigo/projects/mastermind/mastermind-inventory_dev.manifest &&\
#    cocaine-tool profile upload --profile mastermind-inventory.profile -n mastermind2.26-inventory &&\
    cocaine-tool app restart -n mastermind2.26-inventory -r mastermind2.26-inventory &&\
    cocaine-tool app upload --package ../package.tar.gz -n mastermind2.26 --manifest /home/aniam/src/mm/mastermind/mastermind_dev.manifest &&\
    cocaine-tool profile upload -n mastermind2.26 --profile mastermind.profile &&\
    cocaine-tool app restart -n mastermind2.26 -r mastermind2.26 &&\
    cocaine-tool app upload --package ../package.tar.gz -n mastermind2.26-cache --manifest /home/aniam/src/mm/mastermind/mastermind-cache_dev.manifest &&\
    cocaine-tool profile upload -n mastermind2.26-cache --profile mastermind-cache.profile &&\
    cocaine-tool app restart -n mastermind2.26-cache -r mastermind2.26-cache &&\
    rm ../package.tar.gz
